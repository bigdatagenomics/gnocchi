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
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.rdd.association.{ AdditiveLinearAssociation, Association, DominantLinearAssociation }
import org.apache.commons.math3.distribution.TDistribution
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.bdgenomics.formats.avro.Variant
import scala.math.log10

trait LinearSiteRegression[VM <: VariantModel[VM], A <: Association[VM]]
    extends SiteApplication[VM, A] {

  /**
   * Returns Association object with solution to linear regression.
   *
   * Implementation of RegressSite method from [[SiteApplication]] trait. Performs linear regression on a single site.
   * The Site being regressed in this context is the unique pairing of a [[org.bdgenomics.formats.avro.Variant]] object
   * to a [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype]] name. [[org.bdgenomics.formats.avro.Variant]] objects in this
   * context have contigs defined as CHROM_POS_ALT, which uniquely identify a single base.
   *
   * For calculation of the p-value this uses a t-distribution with N-p-1 degrees of freedom. (N = number of samples,
   * p = number of regressors i.e. genotype+covariates+intercept).
   *
   * @param observations Array of tuples. The first element is a coded genotype taken from
   *                     [[net.fnothaft.gnocchi.rdd.genotype.GenotypeState]]. The second is an array of phenotype values
   *                     taken from [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype]] objects. All genotypes are of the same
   *                     site and therefore reference the same contig value i.e. all have the same CHROM_POS_ALT
   *                     identifier. Array of phenotypes has primary phenotype first then covariates.
   * @param variant [[org.bdgenomics.formats.avro.Variant]] being regressed
   * @param phenotype [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype]], The name of the phenotype being regressed.
   * @return [[net.fnothaft.gnocchi.rdd.association.Association]] object containing statistic result for Logistic Regression.
   */
  def applyToSite(observations: Array[(Double, Array[Double])],
                  variant: Variant,
                  phenotype: String,
                  phaseSetId: Int): A = {
    // class for ols: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html

    // transform the data in to design matrix and y matrix compatible with OLSMultipleLinearRegression
    val phenotypesLength = observations(0)._2.length
    val numObservations = observations.length
    val x = new Array[Array[Double]](numObservations)
    val y = new Array[Double](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var sample = new Array[Double](phenotypesLength)
    var runningSum = 0.0
    for (i <- 0 until numObservations) {
      sample = new Array[Double](phenotypesLength)
      sample(0) = observations(i)._1.toDouble
      runningSum += sample(0)
      observations(i)._2.slice(1, phenotypesLength).copyToArray(sample, 1)
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
      val residualDegreesOfFreedom = numObservations - phenotypesLength - 1
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
      constructAssociation(variant.getContigName,
        numObservations,
        "Linear",
        beta,
        genoSE,
        variant,
        phenotype,
        logPValue,
        pvalue,
        phaseSetId,
        statistics)
    } catch {
      case _: SingularMatrixException => constructAssociation(variant.getContigName,
        numObservations,
        "Linear",
        Array(0.0),
        0.0,
        variant,
        phenotype,
        0.0,
        0.0,
        0,
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
                           phaseSetId: Int,
                           statistics: Map[String, Any]): A
}

object AdditiveLinearRegression extends AdditiveLinearRegression {
  val regressionName = "additiveLinearRegression"
}

trait AdditiveLinearRegression extends LinearSiteRegression[AdditiveLinearVariantModel, AdditiveLinearAssociation]
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
                           phaseSetId: Int,
                           statistics: Map[String, Any]): AdditiveLinearAssociation = {
    new AdditiveLinearAssociation(variantId, numSamples, modelType, weights,
      geneticParameterStandardError, variant, phenotype,
      logPValue, pValue, phaseSetId, statistics)
  }
}

object DominantLinearRegression extends DominantLinearRegression {
  val regressionName = "dominantLinearRegression"
}

trait DominantLinearRegression extends LinearSiteRegression[DominantLinearVariantModel, DominantLinearAssociation]
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
                           phaseSetId: Int,
                           statistics: Map[String, Any]): DominantLinearAssociation = {
    new DominantLinearAssociation(variantId, numSamples, modelType, weights,
      geneticParameterStandardError, variant, phenotype,
      logPValue, pValue, phaseSetId, statistics)
  }
}
