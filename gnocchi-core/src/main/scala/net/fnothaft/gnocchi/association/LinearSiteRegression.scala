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
import scala.math.log
import org.apache.commons.math3.distribution.TDistribution

trait LinearSiteRegression extends SiteRegression {

  protected def regressSite(observations: Array[(Int, Array[Double])],
                            locus: ReferenceRegion,
                            altAllele: String,
                            phenotype: String): Association = {
    // taner: fill this in!
    // prolly use this class for linreg: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html

    // transform the data in to design matrix and y matrix compatible with OLSMultipleRegression
    val observationLength = observations(0)._2.length
    val numObservations = observations.length
    var x = new Array[Array[Double]](numObservations)
    var y = new Array[Int](numObservations)

    // loop through observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var sample = new Array[Double](observationLength)
    for (i <- 0 until numObservations){
      sample(i)(0) = observations(i)._1.toDouble
      copy(observations(i), 1, sample, 1, observationLength - 1)
      x(i) = sample
      y(i) = observations(i)._2(0)
    }
    // create linear model
    val LinReg = OLSMultipleLinearRegression()

    // input sample data 
    LinReg.newXSampleData(x)
    LinReg.newYSampleData(y)

    // calculate coefficients
    val beta = LinReg.calculateBeta()

    // compute coefficient variance-covariance matrix
    val varCovar = LinReg.calculateBetaVariance()

    // compute unbiased estimate of variance and standard deviation of phenotype 
    val variance = (LinReg.calculateResidualSumOfSquares()) / (numObservations - observationLength - 1)
    val standardDeviation = sqrt(variance)

    // compute V matrix
    val Vmatrix = varCovar.map(x => x / variance)

    // calculate z-score 
    val zscore = beta(0) / (standardDeviation * sqrt(Vmatrix(0)(0)))

    // calculate p-value and report 
    // under null hypothesis (i.e. the j'th element of beta is 0) the z-score is distributed 
    // as a t-distribution with N-p-1 degrees of freedom.
    val tDist = TDistribution(numObservations - observationLength - 1)
    val pvalue = tDist.cumulativeProbability(zscore)
    val logPValue = log(pvalue)

    // pack up the information into an Association object
    val variant = null
    val statistics = null
    val toReturn = new Association(variant, phenotype, logPValue, statistics)

    return toReturn
  }
}

object AdditiveLinearAssociation extends LinearSiteRegression with Additive {
  val regressionName = "additiveLinearRegression"
}

object DominantLinearAssociation extends LinearSiteRegression with Dominant {
  val regressionName = "dominantLinearRegression"
}
