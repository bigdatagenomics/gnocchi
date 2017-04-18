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
package net.fnothaft.gnocchi.association

import breeze.numerics.log10
import breeze.linalg._
import breeze.numerics._
import net.fnothaft.gnocchi.models.Association
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait LogisticSiteRegression extends SiteRegression {

  /**
   * Returns Association object with solution to logistic regression.
   *
   * Implementation of RegressSite method from SiteRegression trait. Performs logistic regression on a single site.
   * A site in this context is the unique pairing of a [[org.bdgenomics.formats.avro.Variant]] object and a
   * [[net.fnothaft.gnocchi.models.Phenotype]] name. [[org.bdgenomics.formats.avro.Variant]] objects in this context
   * have contigs defined as CHROM_POS_ALT, which uniquely identify a single base.
   *
   * Solves the regression through Newton-Raphson method, then uses the solution to generate p-value.
   *
   * @param observations Array of tuples. The first element is a coded genotype taken from
   *                     [[net.fnothaft.gnocchi.models.GenotypeState]]. The second is an array of phenotype values
   *                     taken from [[net.fnothaft.gnocchi.models.Phenotype]] objects. All genotypes are of the same
   *                     site and therefore reference the same contig value i.e. all have the same CHROM_POS_ALT
   *                     identifier. Array of phenotypes has primary phenotype first then covariates.
   * @param variant [[org.bdgenomics.formats.avro.Variant]] being regressed
   * @param phenotype [[net.fnothaft.gnocchi.models.Phenotype.phenotype]], The name of the phenotype being regressed.
   *
   * @throws [[SingularMatrixException]], repackages any [[breeze.linalg.MatrixSingularException]] into a
   *        [[SingularMatrixException]] for error handling purposes.
   *
   * @return [[net.fnothaft.gnocchi.models.Association]] object containing statistic result for Logistic Regression.
   */
  def regressSite(observations: Array[(Double, Array[Double])],
                  variant: Variant,
                  phenotype: String): Association = {

    /* Setting up logistic regression references */
    val phenotypesLength = observations(0)._2.length
    val numObservations = observations.length
    val lp = new Array[LabeledPoint](numObservations)
    val xixiT = new Array[DenseMatrix[Double]](numObservations)
    val xiVectors = new Array[DenseVector[Double]](numObservations)

    for (i <- observations.indices) {
      val features = new Array[Double](phenotypesLength)
      features(0) = observations(i)._1.toDouble
      observations(i)._2.slice(1, phenotypesLength).copyToArray(features, 1)
      val label = observations(i)._2(0)

      lp(i) = new LabeledPoint(label, new org.apache.spark.mllib.linalg.DenseVector(features))

      val xiVector = DenseVector(1.0 +: features)
      xiVectors(i) = xiVector
      xixiT(i) = xiVector * xiVector.t
    }

    var iter = 0
    val maxIter = 1000
    val tolerance = 1e-6
    var singular = false
    var convergence = false
    val beta = Array.fill[Double](phenotypesLength + 1)(0.0)
    var hessian = DenseMatrix.zeros[Double](phenotypesLength + 1, phenotypesLength + 1)
    val data = lp
    var pi = 0.0

    /* Use Newton-Raphson to solve logistic regression */
    while ((iter < maxIter) && !convergence && !singular) {
      try {
        val logitArray = applyWeights(data, beta)
        val score = DenseVector.zeros[Double](phenotypesLength + 1)
        hessian = DenseMatrix.zeros[Double](phenotypesLength + 1, phenotypesLength + 1)

        for (i <- observations.indices) {
          pi = Math.exp(-logSumOfExponentials(Array(0.0, -logitArray(i))))
          hessian += -xixiT(i) * pi * (1.0 - pi)
          score += xiVectors(i) * (lp(i).label - pi)
        }

        val update = -inv(hessian) * score
        if (max(abs(update)) <= tolerance) {
          convergence = true
        }

        for (j <- beta.indices) {
          beta(j) += update(j)
        }

        if (beta.exists(_.isNaN)) {
          logError("LOG_REG - Broke on iteration: " + iter)
          iter = maxIter
        }
      } catch {
        case _: breeze.linalg.MatrixSingularException => throw new SingularMatrixException()
      }
      iter += 1
    }

    /* Use Hessian and weights to calculate the Wald Statistic, or p-value */
    var matrixSingular = false

    var toRet = new Association(null, null, -9.0, null)
    try {
      val fisherInfo = -hessian
      val fishInv = inv(fisherInfo)
      val standardErrors = sqrt(abs(diag(fishInv)))

      val zScores: DenseVector[Double] = DenseVector(beta) :/ standardErrors
      val zScoresSquared = zScores :* zScores

      val chiDist = new ChiSquaredDistribution(1)
      val probs = zScoresSquared.map(zi => chiDist.cumulativeProbability(zi))

      val waldTests = 1d - probs

      val logWaldTests = waldTests.map(t => log10(t))

      val statistics = Map("numSamples" -> numObservations,
        "weights" -> beta,
        "intercept" -> beta(0),
        "'P Values' aka Wald Tests" -> waldTests,
        "log of wald tests" -> logWaldTests,
        "fisherInfo" -> fisherInfo,
        "XiVectors" -> xiVectors(0),
        "xixit" -> xixiT(0),
        "prob" -> pi,
        "rSquared" -> 0.0)

      Association(variant, phenotype, logWaldTests(1), statistics)
    } catch {
      case _: breeze.linalg.MatrixSingularException => throw new SingularMatrixException()
    }
  }

  /**
   * Apply the weights to data
   *
   * @param lpArray Labeled point array that contains training data
   * @param b Weights vector
   * @return result of multiplying the training data be the weights
   */
  def applyWeights(lpArray: Array[LabeledPoint], b: Array[Double]): Array[Double] = {
    val logitResults = new Array[Double](lpArray.length)
    val bDense = DenseVector(b)
    for (j <- logitResults.indices) {
      val lp = lpArray(j)
      logitResults(j) = DenseVector(1.0 +: lp.features.toArray) dot bDense
    }
    logitResults
  }

  def logSumOfExponentials(exps: Array[Double]): Double = {
    if (exps.length == 1) {
      exps(0)
    }
    val maxExp = max(exps)
    var sums = 0.0
    for (i <- exps.indices) {
      if (exps(i) != 1.2340980408667956E-4) {
        sums += java.lang.Math.exp(exps(i) - maxExp)
      }
    }
    maxExp + Math.log(sums)
  }
}

object AdditiveLogisticAssociation extends LogisticSiteRegression with Additive {
  val regressionName = "additiveLogisticRegression"
}

object DominantLogisticAssociation extends LogisticSiteRegression with Dominant {
  val regressionName = "dominantLogisticRegression"
}
