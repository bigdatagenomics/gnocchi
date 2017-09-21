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
import net.fnothaft.gnocchi.models.{ GnocchiModel, GnocchiModelMetaData }
import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
import net.fnothaft.gnocchi.models.variant.{ QualityControlVariantModel, VariantModel }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel, LogisticVariantModel }
import net.fnothaft.gnocchi.primitives.association.{ LinearAssociation, LogisticAssociation }
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.commons.math3.linear
import org.apache.commons.math3.linear.SingularMatrixException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.formats.avro.Variant

import scala.collection.immutable.Map

trait LogisticSiteRegression[VM <: LogisticVariantModel[VM]] extends SiteRegression[VM] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[VM]

  @throws(classOf[SingularMatrixException])
  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant): LogisticAssociation = {

    val samplesGenotypes = genotypes.samples.map(x => (x.sampleID, List(x.toDouble)))
    val samplesCovariates = phenotypes.map(x => (x._1, x._2.covariates))
    val mergedSampleVector = samplesGenotypes ++ samplesCovariates
    val groupedSampleVector = mergedSampleVector.groupBy(_._1)
    val cleanedSampleVector = groupedSampleVector.mapValues(_.map(_._2).toList.flatten)

    val lp: Array[LabeledPoint] =
      cleanedSampleVector.map(
        x => new LabeledPoint(
          phenotypes(x._1).phenotype.toDouble,
          new org.apache.spark.mllib.linalg.DenseVector(x._2.toArray)))
        .toArray

    val xiVectors = cleanedSampleVector.map(x => DenseVector(1.0 +: x._2.toArray)).toArray
    val xixiT = xiVectors.map(x => x * x.t)

    val phenotypesLength = phenotypes.head._2.covariates.length + 1
    val numObservations = genotypes.samples.length

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

        for (i <- 0 until numObservations) {
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
      val zScores: DenseVector[Double] = DenseVector(beta) /:/ standardErrors
      val waldStats = zScores :* zScores

      // calculate cumulative probs
      val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
      val probs = waldStats.map(zi => {
        chiDist.cumulativeProbability(zi)
      })

      val waldTests = 1d - probs

      val logWaldTests = waldTests.map(t => log10(t))

      LogisticAssociation(
        beta.toList,
        genoStandardError,
        waldTests(1),
        numObservations)
    } catch {
      case _: breeze.linalg.MatrixSingularException => {
        throw new SingularMatrixException()
      }
    }
  }

  /**
   * Apply the weights to data
   *
   * @param lpArray Labeled point array that contains training data
   * @param b       Weights vector
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

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LogisticAssociation): VM
}

object AdditiveLogisticRegression extends AdditiveLogisticRegression {
  val regressionName = "additiveLogisticRegression"
}

trait AdditiveLogisticRegression extends LogisticSiteRegression[AdditiveLogisticVariantModel] with Additive {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[AdditiveLogisticVariantModel] = {

    genotypes.map((genos: CalledVariant) => {
      val association = applyToSite(phenotypes.value, genos)
      constructVM(genos, phenotypes.value.head._2, association)
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LogisticAssociation): AdditiveLogisticVariantModel = {
    AdditiveLogisticVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}

object DominantLogisticRegression extends DominantLogisticRegression {
  val regressionName = "dominantLogisticRegression"
}

trait DominantLogisticRegression extends LogisticSiteRegression[DominantLogisticVariantModel] with Dominant {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            validationStringency: String = "STRICT"): Dataset[DominantLogisticVariantModel] = {

    genotypes.map((genos: CalledVariant) => {
      val association = applyToSite(phenotypes.value, genos)
      constructVM(genos, phenotypes.value.head._2, association)
    })
  }

  protected def constructVM(variant: CalledVariant,
                            phenotype: Phenotype,
                            association: LogisticAssociation): DominantLogisticVariantModel = {
    DominantLogisticVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      phaseSetId = 0)
  }
}
