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

import breeze.linalg._
import breeze.numerics.{ log10, _ }
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import net.fnothaft.gnocchi.rdd.association.{ AdditiveLogisticAssociation, Association, DominantLogisticAssociation }
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.linear
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.formats.avro.Variant

trait LogisticSiteRegression[VM <: VariantModel[VM], A <: Association[VM]] extends SiteApplication[VM, A] {

  /**
   * Returns Association object with solution to logistic regression.
   *
   * Implementation of RegressSite method from SiteRegression trait. Performs logistic regression on a single site.
   * A site in this context is the unique pairing of a [[org.bdgenomics.formats.avro.Variant]] object and a
   * [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype]] name. [[org.bdgenomics.formats.avro.Variant]] objects in this context
   * have contigs defined as CHROM_POS_ALT, which uniquely identify a single base.
   *
   * Solves the regression through Newton-Raphson method, then uses the solution to generate p-value.
   *
   * @param observations Array of tuples. The first element is a coded genotype taken from
   *                     [[net.fnothaft.gnocchi.rdd.genotype.GenotypeState]]. The second is an array of phenotype values
   *                     taken from [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype]] objects. All genotypes are of the same
   *                     site and therefore reference the same contig value i.e. all have the same CHROM_POS_ALT
   *                     identifier. Array of phenotypes has primary phenotype first then covariates.
   * @param variant [[org.bdgenomics.formats.avro.Variant]] being regressed
   * @param phenotype [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype.phenotype]], The name of the phenotype being regressed.
   *
   * @throws [[SingularMatrixException]], repackages any [[breeze.linalg.MatrixSingularException]] into a
   *        [[SingularMatrixException]] for error handling purposes.
   *
   * @return [[net.fnothaft.gnocchi.rdd.association.Association]] object containing statistic result for Logistic Regression.
   */
  @throws(classOf[SingularMatrixException])
  def applyToSite(observations: Array[(Double, Array[Double])],
                  variant: Variant,
                  phenotype: String,
                  phaseSetId: Int): A = {

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
        case _: breeze.linalg.MatrixSingularException => {
          singular = true
          throw new SingularMatrixException()
        }
      }
      iter += 1
    }

    /* Use Hessian and weights to calculate the Wald Statistic, or p-value */
    var matrixSingular = false

    try {
      val fisherInfo = -hessian
      val fishInv = inv(fisherInfo)
      val standardErrors = sqrt(abs(diag(fishInv)))
      val genoStandardError = standardErrors(1)

      // calculate Wald statistic for each parameter in the regression model
      val zScores: DenseVector[Double] = DenseVector(beta) :/ standardErrors
      val waldStats = zScores :* zScores

      // calculate cumulative probs
      val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
      val probs = waldStats.map(zi => {
        chiDist.cumulativeProbability(zi)
      })

      val waldTests = 1d - probs

      val logWaldTests = waldTests.map(t => log10(t))

      val statistics = Map("numSamples" -> numObservations,
        "weights" -> beta,
        "standardError" -> genoStandardError,
        "intercept" -> beta(0),
        "'P Values' aka Wald Tests" -> waldTests,
        "log of wald tests" -> logWaldTests,
        "fisherInfo" -> fisherInfo,
        "XiVectors" -> xiVectors(0),
        "xixit" -> xixiT(0),
        "prob" -> pi,
        "rSquared" -> 0.0)

      constructAssociation(variant.getContigName,
        numObservations,
        "Logistic",
        beta,
        genoStandardError,
        variant,
        phenotype,
        waldTests(1),
        logWaldTests(1),
        phaseSetId,
        statistics)
    } catch {
      case _: breeze.linalg.MatrixSingularException => {
        throw new SingularMatrixException()
      }
      //        matrixSingular = true
      //        constructAssociation(variant.getContig.getContigName,
      //          numObservations,
      //          "Logistic",
      //          beta,
      //          1.0,
      //          variant,
      //          phenotype,
      //          0.0,
      //          0.0,
      //          Map(
      //            "numSamples" -> 0,
      //            "weights" -> beta,
      //            "intercept" -> 0.0,
      //            "'P Values' aka Wald Tests" -> 0.0,
      //            "log of wald tests" -> 0.0,
      //            "fisherInfo" -> 0.0,
      //            "XiVectors" -> xiVectors(0),
      //            "xixit" -> xixiT(0),
      //            "prob" -> pi,
      //            "rSquared" -> 0.0))
      //      }
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

object AdditiveLogisticRegression extends AdditiveLogisticRegression {
  val regressionName = "additiveLogisticRegression"
}
trait AdditiveLogisticRegression extends LogisticSiteRegression[AdditiveLogisticVariantModel, AdditiveLogisticAssociation] with Additive {
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
                           statistics: Map[String, Any]): AdditiveLogisticAssociation = {
    new AdditiveLogisticAssociation(variantId, numSamples, modelType, weights, geneticParameterStandardError,
      variant, phenotype, logPValue, pValue, phaseSetId, statistics)
  }
}

object DominantLogisticRegression extends DominantLogisticRegression {
  val regressionName = "dominantLogisticRegression"
}

trait DominantLogisticRegression extends LogisticSiteRegression[DominantLogisticVariantModel, DominantLogisticAssociation] with Dominant {
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
                           statistics: Map[String, Any]): DominantLogisticAssociation = {
    new DominantLogisticAssociation(variantId, numSamples, modelType, weights, geneticParameterStandardError,
      variant, phenotype, logPValue, pValue, phaseSetId, statistics)
  }
}
