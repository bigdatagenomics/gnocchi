/**
 * Copyright 2015 Taner Dagdelen
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

import breeze.numerics.log10
import breeze.linalg._
import breeze.numerics._
import net.fnothaft.gnocchi.models.Association
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SQLContext
import org.apache.commons.math3.special.Gamma
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait LogisticSiteRegression extends SiteRegression {

  /**
   * This method will perform logistic regression on a single site.
   *
   * @param observations An array containing tuples in which the first element is the coded genotype. The second is an Array[Double] representing the phenotypes, where the first element in the array is the phenotype to regress and the rest are to be treated as covariates. .
   * @param locus        A ReferenceRegion object representing the location in the genome of the site.
   * @param altAllele    A String specifying the value of the alternate allele that makes up the variant or SNP
   * @param phenotype    The name of the phenotype being regressed.
   * @return The Association object that results from the linear regression
   */

  def regressSite(observations: Array[(Double, Array[Double])],
                  locus: ReferenceRegion,
                  altAllele: String,
                  phenotype: String,
                  scOption: Option[SparkContext]): Association = {

    // transform the data in to design matrix and y matrix compatible with mllib's logistic regresion
    val observationLength = observations(0)._2.length
    val numObservations = observations.length
    val lp = new Array[LabeledPoint](numObservations)
    val xixiT = new Array[DenseMatrix[Double]](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var features = new Array[Double](observationLength)
    for (i <- observations.indices) {
      // rearrange variables into label and features
      features = new Array[Double](observationLength)
      features(0) = observations(i)._1.toDouble
      observations(i)._2.slice(1, observationLength).copyToArray(features, 1)
      val label = observations(i)._2(0)

      // pack up info into LabeledPoint object
      lp(i) = new LabeledPoint(label, new org.apache.spark.mllib.linalg.DenseVector(features))

      // compute xi*xi.t for hessian matrix
      val xiVector = DenseVector(1.0 +: features)
      xixiT(i) = xiVector * xiVector.t
    }

    assert(scOption.isDefined, "Spark Context must be fed into the logistic regression apply function as parameter scOption.")
    val sc = scOption.get

    // convert the labeled point RDD into a dataFrame for mllib's LogisticRegression
    val sqlCtx = SQLContext.getOrCreate(sc)
    import sqlCtx.implicits._
    val lpDF = sc.parallelize(lp).toDF

    // feed in the lp dataframe into the logistic regression solver and get the weights
    val logReg = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setStandardization(true) // is this necessary?
      .setTol(.001) // arbitrarily set at 1e-3
      .setMaxIter(100) // arbitrarily set to 100
      .setFitIntercept(true)
      .setThreshold(0.5)
      .setStandardization(true)
    val logRegModel = logReg.fit(lpDF)
    val b: Array[Double] = logRegModel.intercept +: logRegModel.coefficients.toArray

    /// CALCULATE WALD STATISTIC "P Value" ///

    // calculate the logit for each xi
    val data = lp
    val logitArray = logit(data, b)

    // calculate the hessian
    val hessian = DenseMatrix.zeros[Double](observationLength + 1, observationLength + 1)
    for (i <- observations.indices) {
      val pi = Math.exp(logitArray(i)) / (1 + Math.exp(logitArray(i)))
      hessian += -xixiT(i) * pi * (1 - pi) // See ESL, page 120
    }

    // calculate the standard error for the genotypic predictor
    val fisherInfo = -hessian
    val fishInv = inv(fisherInfo)
    val standardErrors = sqrt(diag(fishInv))

    // calculate Wald z-scores
    val zScores: DenseVector[Double] = DenseVector(b) :/ standardErrors
    val zScoresSquared = zScores :* zScores

    // calculate cumulative probs
    val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
    val probs = zScoresSquared.map(zi => { chiDist.cumulativeProbability(zi) })

    // calculate wald test statistics
    val waldTests = 1d - probs

    // calculate the log of the p-value for the genetic component
    val logWaldTests = waldTests.map(t => { log10(t) })

    // pack up the information into an Association object
    val variant = new Variant()
    val contig = new Contig()
    contig.setContigName(locus.referenceName)
    variant.setContig(contig)
    variant.setStart(locus.start)
    variant.setEnd(locus.end)
    variant.setAlternateAllele(altAllele)
    val statistics = Map("weights" -> b,
      "intercept" -> b(0),
      "'P Values' aka Wald Tests" -> waldTests,
      "log of wald tests" -> logWaldTests)
    Association(variant, phenotype, waldTests(1), statistics)
  }

  def logit(lpArray: Array[LabeledPoint], b: Array[Double]): Array[Double] = {
    val logitResults = new Array[Double](lpArray.length)
    val bDense = DenseVector(b)
    for (j <- logitResults.indices) {
      val lp = lpArray(j)
      logitResults(j) = DenseVector(1.0 +: lp.features.toArray) dot bDense
    }
    logitResults
  }
}

object AdditiveLogisticAssociation extends LogisticSiteRegression with Additive {
  val regressionName = "additiveLogisticRegression"
}

object DominantLogisticAssociation extends LogisticSiteRegression with Dominant {
  val regressionName = "dominantLogisticRegression"
}