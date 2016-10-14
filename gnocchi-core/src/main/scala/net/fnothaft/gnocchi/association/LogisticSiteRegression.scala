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
import net.fnothaft.gnocchi.models.Association
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.Param
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.commons.math3.distribution.NormalDistribution
import org.bdgenomics.formats.avro.{ Variant, Contig }


trait LogisticSiteRegression extends SiteRegression {

  /**
   * This method will perform logistic regression on a single site.
   * @param observations An array containing tuples in which the first element is the coded genotype. The second is an Array[Double] representing the phenotypes, where the first element in the array is the phenotype to regress and the rest are to be treated as covariates. .
   * @param locus A ReferenceRegion object representing the location in the genome of the site.
   * @param altAllele A String specifying the value of the alternate allele that makes up the variant or SNP
   * @param phenotype The name of the phenotype being regressed.
   * @return The Association object that results from the linear regression
   */

  def regressSite(sc: SparkContext,
                  observations: Array[(Double, Array[Double])],
                  locus: ReferenceRegion,
                  altAllele: String,
                  phenotype: String): Association = {
    // class for ols: org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
    // see http://commons.apache.org/proper/commons-math/javadocs/api-3.6.1/org/apache/commons/math3/stat/regression/OLSMultipleLinearRegression.html

    // transform the data in to design matrix and y matrix compatible with OLSMultipleLinearRegression
    val observationLength = observations(0)._2.length
    val numObservations = observations.length
    val lp = new Array[LabeledPoint](numObservations)
    val xiSq = new Array[Double](numObservations)
//    val x = new Array[Array[Double]](numObservations)
//    var y = new Array[Double](numObservations)

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var sample = new Array[Double](observationLength)
    var genoSum = 0.0
    for (i <- 0 until numObservations) {
      sample = new Array[Double](observationLength)
      sample(0) = observations(i)._1.toDouble
      observations(i)._2.slice(1, observationLength).copyToArray(sample, 1)

      // pack up info into LabeledPoint object
      lp(i) = new LabeledPoint(observations(i)._2(0), new DenseVector(sample))

      // calculate Xi0^2 for each xi
      xiSq(i) = Math.pow(sample(0), 2)
    }

    // convert the labeled point RDD into a dataFrame
    val sqlCtx = SQLContext.getOrCreate(sc)
    import sqlCtx.implicits._
    val lpDF = sc.parallelize(lp).toDF

    // feed in the lp dataframe into the logistic regression solver and get the weights
    val logReg = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("labels")
      .setStandardization(true) // is this necessary?
      .setTol(.001) // arbitrarily set at 1e-3
      .setMaxIter(100) // arbitrarily set to 100
    val logRegModel = logReg.fit(lpDF)

    /// USE WALD STATISTIC TO CALCULATE "P Value" ///

    // calculate the logit for each xi
    val data = lp
    val logitArray = logit(data, logRegModel)

    // calculate the probability for each xi
    val probTimesXiArray = Array[Double](observations.length)
    for (i <- 0 to observations.length) {
      val pi = Math.exp(logitArray(i))/(1 + Math.exp(logitArray(i)))
      probTimesXiArray(i) = xiSq(i)*pi*(1-pi)
    }

    // calculate the standard error for the genotypic predictor
    val standardError = probTimesXiArray.sum

    // calculate Beta_p/SEp
    val w = logRegModel.coefficients(0)/standardError

    // use normal distribution to get the "p value"
    val normDist = new NormalDistribution()
    val waldTest = 1.0 - normDist.cumulativeProbability(w)
    val logWaldTest = log10(waldTest)

    // pack up the information into an Association object
    val variant = new Variant()
    val contig = new Contig()
    contig.setContigName(locus.referenceName)
    variant.setContig(contig)
    variant.setStart(locus.start)
    variant.setEnd(locus.end)
    variant.setAlternateAllele(altAllele)
    val statistics = Map("'P Value' aka Wald Test" -> waldTest, "log of wald test" -> logWaldTest)
    Association(variant, phenotype, logWaldTest, statistics)
  }

  def logit(lpArray: Array[LabeledPoint], model: LogisticRegressionModel): Array[Double] = {
    val logitResults = Array[Double](lpArray.length)
    for (j <- 0 to lpArray.length) {
      var res = 0.0
      val lp = lpArray(j)
      for (i <- 0 to lp.features.size) {
        res += lp.features(i)*model.coefficients(i)
      }
      logitResults(j) = res
    }
    logitResults
  }

object AdditiveLogisticAssociation extends LinearSiteRegression with Additive {
  val regressionName = "additiveLinearRegression"
}

object DominantLogisticAssociation extends LinearSiteRegression with Dominant {
  val regressionName = "dominantLinearRegression"
}