/**
 * Copyright 2016 Frank Austin Nothaft, Taner Dagdelen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.Association
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.bdgenomics.adam.models.ReferenceRegion
import scala.math.sqrt
import scala.math.log10
import org.apache.commons.math3.distribution.TDistribution

trait LinearSiteRegression extends SiteRegression {

  /**
   * This method will perform linear regression on a single site.
   * @param observations An array containing tuples in which the first element is the coded genotype. The second is an Array[Double] representing the phenotypes, where the first element in the array is the phenotype to regress and the rest are to be treated as covariates. .
   * @param locus A ReferenceRegion object representing the location in the genome of the site.
   * @param altAllele A String specifying the value of the alternate allele that makes up the variant or SNP
   * @param phenotype The name of the phenotype being regressed.
   * @return The Association object that results from the linear regression
   */
  def regressSite(observations: Array[(Double, Array[Double])],
                  locus: ReferenceRegion,
                  altAllele: String,
                  phenotype: String): Association = {
    // class for ols: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html

    // transform the data in to design matrix and y matrix compatible with OLSMultipleLinearRegression
    val observationLength = observations(0)._2.length
    val numObservations = observations.length
    val x = new Array[Array[Double]](numObservations)
    var y = new Array[Double](numObservations)

    // loop through observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var sample = new Array[Double](observationLength)
    var genoSum = 0.0
    for (i <- 0 until numObservations) {
      sample = new Array[Double](observationLength)
      sample(0) = observations(i)._1.toDouble
      observations(i)._2.slice(1, observationLength).copyToArray(sample, 1)
      x(i) = sample
      y(i) = observations(i)._2(0)
    }

    // create linear model
    val ols = new OLSMultipleLinearRegression()

    // input sample data 
    ols.newSampleData(y, x)

    // calculate coefficients
    val beta = ols.estimateRegressionParameters()

    // calculate Rsquared
    val rSquared = ols.calculateRSquared()

    // compute the regression parameters standard errors
    val standardErrors = ols.estimateRegressionParametersStandardErrors()

    // get standard error for genotype parameter for p value calculation
    val genoSE = standardErrors(1)

    // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
    val t = beta(1) / genoSE

    /* calculate p-value and report: 
    / Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is 
    / a t-distribution with N-p-1 degrees of freedom.
    */
    val tDist = new TDistribution(numObservations - observationLength - 1)
    val pvalue = 1.0 - tDist.cumulativeProbability(t)
    val logPValue = log10(pvalue)

    // pack up the information into an Association object
    val variant = null
    val statistics = Map("rSquared" -> rSquared)
    val phenotype = null
    val associationObject = new Association(variant, phenotype, logPValue, statistics)

    return associationObject
  }
}

object AdditiveLinearAssociation extends LinearSiteRegression with Additive {
  val regressionName = "additiveLinearRegression"
}

object DominantLinearAssociation extends LinearSiteRegression with Dominant {
  val regressionName = "dominantLinearRegression"
}
