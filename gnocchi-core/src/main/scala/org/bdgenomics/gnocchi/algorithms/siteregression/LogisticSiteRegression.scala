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
package org.bdgenomics.gnocchi.algorithms.siteregression

import breeze.linalg._
import breeze.numerics._
import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel

import scala.annotation.tailrec
import scala.collection.immutable.Map

trait LogisticSiteRegression extends SiteRegression[LogisticVariantModel, LogisticAssociation] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            allelicAssumption: String = "ADDITIVE",
            validationStringency: String = "STRICT"): Dataset[LogisticVariantModel] = {

    import genotypes.sqlContext.implicits._

    genotypes.flatMap((genos: CalledVariant) => {
      try {
        val association = applyToSite(phenotypes.value, genos, allelicAssumption)
        Some(constructVM(genos, phenotypes.value.head._2, association, allelicAssumption))
      } catch {
        case e: breeze.linalg.MatrixSingularException => {
          logError(e.toString)
          None
        }
      }
    })
  }

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant,
                  allelicAssumption: String): LogisticAssociation = {

    val (data, labels) = prepareDesignMatrix(phenotypes, genotypes, allelicAssumption)

    val numObservations = genotypes.samples.count(_.misses == 0)

    val maxIter = 1000
    val tolerance = 1e-6
    val initBeta = DenseVector.zeros[Double](data.cols)

    val (beta, hessian) = findBeta(data, labels, initBeta, maxIter = maxIter, tolerance = tolerance)

    // Use Hessian and weights to calculate the Wald Statistic, or p-value
    val fisherInfo = -hessian
    val fishInv = inv(fisherInfo)
    val standardErrors = sqrt(abs(diag(fishInv)))
    val genoStandardError = standardErrors(1)

    // calculate Wald statistic for each parameter in the regression model
    val zScores: DenseVector[Double] = DenseVector(beta) /:/ standardErrors
    val waldStats = zScores *:* zScores

    // calculate cumulative probs
    val chiDist = new ChiSquaredDistribution(1) // 1 degree of freedom
    val probs = waldStats.map(zi => chiDist.cumulativeProbability(zi))

    val waldTests = 1d - probs

    LogisticAssociation(
      beta.toList,
      genoStandardError,
      waldTests(1),
      numObservations)
  }

  /**
   * Tail recursive training function that finds the optimal weights vector given the input training data.
   *
   * @note DO NOT place any statements after the final recursive call to itself, or it will break tail recursive speed
   *       up provided by the scala compiler.
   *
   * @param X [[breeze.linalg.DenseMatrix]] design matrix of [[Double]] that contains training data
   * @param Y [[breeze.linalg.DenseVector]] of labels that contain labels for parameter X
   * @param beta Weights vector
   * @param iter current iteration, used for recursive tracking
   * @param maxIter maximum number of iterations to be used for recursive depth limiting
   * @param tolerance smallest allowable step size before function
   * @return tuple where first item are weight values, beta, as [[Array]]
   *         and second is Hessian matrix as [[DenseMatrix]]
   */
  @tailrec
  final def findBeta(X: DenseMatrix[Double],
                     Y: DenseVector[Double],
                     beta: DenseVector[Double],
                     iter: Int = 0,
                     maxIter: Int = 1000,
                     tolerance: Double = 1e-6): (Array[Double], DenseMatrix[Double]) = {

    val logitArray = X * beta

    // column vector containing probabilities of samples being in class 1 (a case / affected / a positive indicator)
    val p = sigmoid(logitArray)

    // (Xi is a single sample's row) Xi.T * Xi * pi * (1 - pi) is a nXn matrix, that we sum across all i
    val hessian = p.toArray.zipWithIndex.map { case (pi, i) => -X(i, ::).t * X(i, ::) * pi * (1.0 - pi) }.reduce(_ + _)

    // subtract predicted probability from actual response and multiply each row by the error for that sample. Achieved
    // by getting error (Y-p) and copying it columnwise N times (N = number of columns in X) and using *:* to pointwise
    // multiply the resulting matrix with X
    val sampleScore = { X *:* tile(Y - p, 1, X.cols) }

    // sum into one column
    val score = sum(sampleScore(::, *)).t

    val update = -inv(hessian) * score
    val updatedBeta = beta + update

    if (updatedBeta.exists(_.isNaN)) logError("LOG_REG - Broke on iteration: " + iter)
    if (max(abs(update)) <= tolerance || iter + 1 == maxIter) return (updatedBeta.toArray, hessian)

    findBeta(X, Y, updatedBeta, iter = iter + 1, maxIter = maxIter, tolerance = tolerance)
  }

  /**
   * Data preparation function that converts the gnocchi models into breeze linear algebra primitives BLAS/LAPACK
   * optimizations.
   *
   * @param phenotypes [[Phenotype]]s map that contains the labels (primary phenotype) and part of the design matrix
   *                  (covariates)
   * @param genotypes [[CalledVariant]] object to convert into a breeze design matrix
   * @return tuple where first element is the [[DenseMatrix]] design matrix and second element
   *         is [[DenseVector]] of labels
   */
  def prepareDesignMatrix(phenotypes: Map[String, Phenotype],
                          genotypes: CalledVariant,
                          allelicAssumption: String): (DenseMatrix[Double], DenseVector[Double]) = {

    val validGenos = genotypes.samples.filter(genotypeState => genotypeState.misses == 0 && phenotypes.contains(genotypeState.sampleID))

    val samplesGenotypes = allelicAssumption.toUpperCase match {
      case "ADDITIVE"  => validGenos.map(genotypeState => (genotypeState.sampleID, List(genotypeState.additive)))
      case "DOMINANT"  => validGenos.map(genotypeState => (genotypeState.sampleID, List(genotypeState.dominant)))
      case "RECESSIVE" => validGenos.map(genotypeState => (genotypeState.sampleID, List(genotypeState.recessive)))
    }

    val cleanedSampleVector = samplesGenotypes
      .map { case (sampleID, genotype) => (sampleID, (genotype ++ phenotypes(sampleID).covariates).toList) }

    val XandY = cleanedSampleVector.map { case (sampleID, sampleVector) => (DenseVector(1.0 +: sampleVector.toArray), phenotypes(sampleID).phenotype) }
    val X = DenseMatrix(XandY.map { case (sampleVector, sampleLabel) => sampleVector }: _*)
    val Y = DenseVector(XandY.map { case (sampleVector, sampleLabel) => sampleLabel }: _*)
    (X, Y)
  }

  def constructVM(variant: CalledVariant,
                  phenotype: Phenotype,
                  association: LogisticAssociation,
                  allelicAssumption: String): LogisticVariantModel = {
    LogisticVariantModel(variant.uniqueID,
      association,
      phenotype.phenoName,
      variant.chromosome,
      variant.position,
      variant.referenceAllele,
      variant.alternateAllele,
      allelicAssumption,
      phaseSetId = 0)
  }
}

object LogisticSiteRegression extends LogisticSiteRegression {
  val regressionName = "LogisticSiteRegression"
}