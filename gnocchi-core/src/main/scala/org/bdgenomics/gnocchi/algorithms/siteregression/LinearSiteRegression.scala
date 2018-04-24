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

import org.bdgenomics.gnocchi.primitives.association.{ LinearAssociation, LinearAssociationBuilder }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.StudentsT
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, PhenotypesContainer }
import org.bdgenomics.gnocchi.utils.Timers._

import scala.collection.immutable.Map

trait LinearSiteRegression extends SiteRegression[LinearVariantModel, LinearAssociation] {

  /**
   * Return a [[LinearRegressionResults]] object for the passed genotypes and phenotypesContainer
   *
   * @param genotypes [[GenotypeDataset]] to be analyzed in this study
   * @param phenotypesContainer [[PhenotypesContainer]] corresponding to the genotype data
   * @return [[LinearRegressionResults]] storing access to the models, associations or a
   *        [[org.bdgenomics.gnocchi.models.LinearGnocchiModel]]
   */
  def apply(genotypes: GenotypeDataset,
            phenotypesContainer: PhenotypesContainer): LinearRegressionResults = {
    LinearRegressionResults(genotypes, phenotypesContainer)
  }

  /**
   * Run Linear Regression over a [[Dataset]] of [[CalledVariant]] objects and return a tuple that
   * contains a [[Dataset]] of the resulting [[LinearVariantModel]] and a [[Dataset]] of the
   * [[LinearAssociation]]
   *
   * @param genotypes [[Dataset]] of [[CalledVariant]]s for which to compute Linear models and
   *                 associations
   * @param phenotypes Phenotype data corresponding to the [[Dataset]] of [[CalledVariant]] objects
   * @param allelicAssumption Allelic assumption to use for this analysis
   * @return Tuple of two [[Dataset]] objects. First is [[LinearVariantModel]] objects, second is
   *         [[LinearAssociation]] objects
   */
  def createModelAndAssociations(genotypes: Dataset[CalledVariant],
                                 phenotypes: Broadcast[Map[String, Phenotype]],
                                 allelicAssumption: String): (Dataset[LinearVariantModel], Dataset[LinearAssociation]) = CreateModelAndAssociations.time {

    import genotypes.sqlContext.implicits._

    //ToDo: Singular Matrix Exceptions
    val results = genotypes.flatMap((genos: CalledVariant) => {
      try {
        val (model, association) = applyToSite(genos, phenotypes.value, allelicAssumption)
        Some((model, association))
      } catch {
        case e: breeze.linalg.MatrixSingularException => None
      }
    })

    (results.map(_._1), results.map(_._2))
  }

  /**
   *
   * @param genotypesDS [[GenotypeDataset]] that wraps a dataset of genomic data
   * @param phenotypesContainer [[PhenotypesContainer]] that contains phenotypic information
   * @return [[Dataset]] of [[LinearAssociation]] that store the results
   */
  def createAssociationsDataset(genotypesDS: GenotypeDataset,
                                phenotypesContainer: PhenotypesContainer): Dataset[LinearAssociation] = CreatAssociationsDataset.time {

    import genotypesDS.genotypes.sqlContext.implicits._

    //ToDo: Singular Matrix Exceptions
    val results = genotypesDS.genotypes.flatMap((genos: CalledVariant) => {
      try {
        val association = solveAssociation(genos, phenotypesContainer.phenotypes.value, genotypesDS.allelicAssumption)
        Some(association)
      } catch {
        case e: breeze.linalg.MatrixSingularException => None
      }
    })

    results
  }

  /**
   * Solve the Logistic Regression problem for a single variant site.
   *
   * @param genotypes a single [[CalledVariant]] object to solve Logistic Regression for
   * @param phenotypes Phenotypes corresponding to the genotype data
   * @param allelicAssumption Allelic assumption to use for the genotype encoding
   * @return a tuple of [[LinearVariantModel]] and [[LinearAssociation]] containing the relevant
   *         statistics for the Logistic Regression solution
   */
  def solveAssociation(genotypes: CalledVariant,
                       phenotypes: Map[String, Phenotype],
                       allelicAssumption: String): LinearAssociation = {

    val (x, y) = prepareDesignMatrix(genotypes, phenotypes, allelicAssumption)

    val (xTx, xTy, beta) = solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = calculateSignificance(x, y, beta, xTx)

    LinearAssociation(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      x.rows,
      pValue,
      genoSE,
      ssResiduals,
      t)
  }

  /**
   * Solve the Logistic Regression problem for a single variant site.
   *
   * @param genotypes a single [[CalledVariant]] object to solve Logistic Regression for
   * @param phenotypes Phenotypes corresponding to the genotype data
   * @param allelicAssumption Allelic assumption to use for the genotype encoding
   * @return a tuple of [[LinearVariantModel]] and [[LinearAssociation]] containing the relevant
   *         statistics for the Logistic Regression solution
   */
  def applyToSite(genotypes: CalledVariant,
                  phenotypes: Map[String, Phenotype],
                  allelicAssumption: String): (LinearVariantModel, LinearAssociation) = {

    val (x, y) = prepareDesignMatrix(genotypes, phenotypes, allelicAssumption)

    val (xTx, xTy, beta) = solveRegression(x, y)

    val (genoSE, t, pValue, ssResiduals) = calculateSignificance(x, y, beta, xTx)

    val association = LinearAssociation(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      x.rows,
      pValue,
      genoSE,
      ssResiduals,
      t)

    val model = LinearVariantModel(
      genotypes.uniqueID,
      genotypes.chromosome,
      genotypes.position,
      genotypes.referenceAllele,
      genotypes.alternateAllele,
      x.rows,
      x.cols,
      xTx.toArray,
      xTy.toArray,
      x.rows - x.cols,
      beta.data.toList)

    (model, association)
  }

  /**
   * This function solves the equation Y = Xβ for β and gives back relevant matrices in a tuple.
   * Places the actual regression solution in a separate function for testing reasons.
   *
   * @param x formatted x matrix
   * @param y formatted y vector
   * @return (xTx matrix, xTy vector, weights vector)
   */
  def solveRegression(x: DenseMatrix[Double],
                      y: DenseVector[Double]): (DenseMatrix[Double], DenseVector[Double], DenseVector[Double]) = {
    val xTx = x.t * x // p x p matrix
    val xTy = x.t * y // p x 1 vector

    // This line is the breeze notation for the equation xTx*beta = xTy, solve for beta
    val beta = xTx \ xTy

    (xTx, xTy, beta)
  }

  /**
   * Given a weight vector, and input X and Y, solve for the significance value of the genotype
   * parameter. Also takes in optional partial sum of square residuals and additional samples count
   * for the purpose of merging together an exsiting association with a new set of data.
   *
   * @param x Design matrix to be used, containing formatted genotype data
   * @param y Target vector corresponding to the design matrix
   * @param beta weight vector to use for calculating the genotype parameter signicicance
   * @param modelxTx the xTx matrix of the model that is being significance tested
   * @param partialSSResiduals Optional parameter that can be used as the SSResiduals from
   *                           another/other datasets
   * @return (genotype parameter standard error, t-statistic for model, pValue for model, sum of
   *         squared residuals)
   */
  def calculateSignificance(x: DenseMatrix[Double],
                            y: DenseVector[Double],
                            beta: DenseVector[Double],
                            modelxTx: DenseMatrix[Double],
                            partialSSResiduals: Option[Double] = None,
                            additionalSamples: Option[Int] = None): (Double, Double, Double, Double) = {

    require(partialSSResiduals.isDefined == additionalSamples.isDefined,
      "You need to either define both partialSSResiduals and additionalNumSamples, or neither.")

    val residuals = y - (x * beta)
    val ssResiduals = residuals.t * residuals + partialSSResiduals.getOrElse(0: Double)

    // compute the regression parameters standard errors
    val betaVariance = diag(inv(modelxTx))
    val sigma = ssResiduals / (additionalSamples.getOrElse(0: Int) + x.rows - x.cols)
    val standardErrors = sqrt(sigma * betaVariance)

    // get standard error for genotype parameter (for p value calculation)
    val genoSE = standardErrors(1)

    // test statistic t for jth parameter is equal to bj/SEbj, the parameter estimate divided by its standard error
    val t = beta(1) / genoSE

    /* calculate p-value and report:
      Under null hypothesis (i.e. the j'th element of weight vector is 0) the relevant distribution is
      a t-distribution with N-p degrees of freedom.
      (N = number of samples, p = number of regressors i.e. genotype+covariates+intercept)
      https://en.wikipedia.org/wiki/T-statistic
    */
    val residualDegreesOfFreedom = additionalSamples.getOrElse(0: Int) + x.rows - x.cols
    val tDist = StudentsT(residualDegreesOfFreedom)
    val pValue = 2 * tDist.cdf(-math.abs(t))
    (genoSE, t, pValue, ssResiduals)
  }
}

object LinearSiteRegression extends LinearSiteRegression