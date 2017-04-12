/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi.algorithms.siteregression

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.rdd.association.{ AdditiveLinearAssociation, Association, DominantLinearAssociation }
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.bdgenomics.formats.avro.Variant

import scala.math.log10

trait LinearSiteRegression[VM <: VariantModel[VM]] extends SiteApplication[VM] {

  /**
   * This method will perform linear regression on a single site.
   * @param observations An array containing tuples in which the first element is the coded genotype. The second is an Array[Double] representing the phenotypes, where the first element in the array is the phenotype to regress and the rest are to be treated as covariates.
   * @param variant The variant that is being regressed.
   * @param phenotype The name of the phenotype being regressed.
   * @return The Association object that results from the linear regression
   */
  def applyToSite(observations: Array[(Double, Array[Double])],
                  variant: Variant,
                  phenotype: String): Association[VM] = {
    // class for ols: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html

    // transform the data in to design matrix and y matrix compatible with OLSMultipleLinearRegression
    val observationLength = observations(0)._2.length
    val numObservations = observations.length
    val x = new Array[Array[Double]](numObservations)
    val y = new Array[Double](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var sample = new Array[Double](observationLength)
    var runningSum = 0.0
    for (i <- 0 until numObservations) {
      sample = new Array[Double](observationLength)
      sample(0) = observations(i)._1.toDouble
      runningSum += sample(0)
      observations(i)._2.slice(1, observationLength).copyToArray(sample, 1)
      x(i) = sample
      y(i) = observations(i)._2(0)
    }
    val mean = runningSum / numObservations.toDouble

    try {
      // create linear model
      val ols = new OLSMultipleLinearRegression()

      // input sample data
      ols.newSampleData(y, x)

      // calculate coefficients
      val beta = ols.estimateRegressionParameters()

      // calculate sum of squared residuals
      val ssResiduals = ols.calculateResidualSumOfSquares()

      // calculate sum of squared deviations
      val ssDeviations = sumOfSquaredDeviations(observations, mean)

      // calculate Rsquared
      val rSquared = ols.calculateRSquared()

      // compute the regression parameters standard errors
      val standardErrors = ols.estimateRegressionParametersStandardErrors()

      // get standard error for genotype parameter (for p value calculation)
      val genoSE = standardErrors(1)

      // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
      val t = beta(1) / genoSE

      /* calculate p-value and report:
        Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is
        a t-distribution with N-p-1 degrees of freedom. (N = number of samples, p = number of regressors i.e. genotype+covariates+intercept)
        https://en.wikipedia.org/wiki/T-statistic
      */
      val residualDegreesOfFreedom = numObservations - observationLength - 1
      val tDist = new TDistribution(residualDegreesOfFreedom)
      val pvalue = 2 * tDist.cumulativeProbability(-math.abs(t))
      val logPValue = log10(pvalue)

      val statistics = Map("rSquared" -> rSquared,
        "weights" -> beta,
        "intercept" -> beta(0),
        "numSamples" -> numObservations,
        "ssDeviations" -> ssDeviations,
        "ssResiduals" -> ssResiduals,
        "tStatistic" -> t,
        "residualDegreesOfFreedom" -> residualDegreesOfFreedom)
      constructAssociation(variant.getContig.getContigName,
        numObservations,
        "Linear",
        beta,
        genoSE,
        variant,
        phenotype,
        logPValue,
        pvalue,
        statistics)
    } catch {
      case _: SingularMatrixException => constructAssociation(variant.getContig.getContigName,
        numObservations,
        "Linear",
        Array(0.0),
        0.0,
        variant,
        phenotype,
        0.0,
        0.0,
        Map())
    }
  }

  def sumOfSquaredDeviations(observations: Array[(Double, Array[Double])], mean: Double): Double = {
    val squaredDeviations = observations.map(p => math.pow(p._1.toDouble - mean, 2))
    squaredDeviations.sum
  }

  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           statistics: Map[String, Any]): Association[VM]
}

object AdditiveLinearRegression extends AdditiveLinearRegression {
  val regressionName = "additiveLinearRegression"
}

trait AdditiveLinearRegression extends LinearSiteRegression[AdditiveLinearVariantModel]
    with Additive {
  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           statistics: Map[String, Any]): AdditiveLinearAssociation = {
    new AdditiveLinearAssociation(variantId, numSamples, modelType, weights,
      geneticParameterStandardError, variant, phenotype,
      logPValue, pValue, statistics)
  }
}

object DominantLinearRegression extends DominantLinearRegression {
  val regressionName = "dominantLinearRegression"
}

trait DominantLinearRegression extends LinearSiteRegression[AdditiveLinearVariantModel]
    with Dominant {
  def constructAssociation(variantId: String,
                           numSamples: Int,
                           modelType: String,
                           weights: Array[Double],
                           geneticParameterStandardError: Double,
                           variant: Variant,
                           phenotype: String,
                           logPValue: Double,
                           pValue: Double,
                           statistics: Map[String, Any]): DominantLinearAssociation = {
    new DominantLinearAssociation(variantId, numSamples, modelType, weights,
      geneticParameterStandardError, variant, phenotype,
      logPValue, pValue, statistics)
  }
}
