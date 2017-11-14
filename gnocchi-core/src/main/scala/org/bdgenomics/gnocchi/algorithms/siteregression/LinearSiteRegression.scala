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

import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import breeze.linalg._
import breeze.numerics._
import breeze.stats._
import breeze.stats.distributions.StudentsT
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel

import scala.collection.immutable.Map

trait LinearSiteRegression extends SiteRegression[LinearVariantModel, LinearAssociation] {

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            allelicAssumption: String = "ADDITIVE",
            validationStringency: String = "STRICT"): Dataset[LinearVariantModel] = {

    import genotypes.sqlContext.implicits._

    //ToDo: Singular Matrix Exceptions
    genotypes.flatMap((genos: CalledVariant) => {
      try {
        val association = applyToSite(phenotypes.value, genos, allelicAssumption)
        Some(constructVM(genos, phenotypes.value.head._2, association, allelicAssumption))
      } catch {
        case e: breeze.linalg.MatrixSingularException => None
      }
    })
  }

  def applyToSite(phenotypes: Map[String, Phenotype],
                  genotypes: CalledVariant,
                  allelicAssumption: String): LinearAssociation = {

    val (x, y) = prepareDesignMatrix(genotypes, phenotypes, allelicAssumption)

    // TODO: Determine if QR factorization is faster
    val beta = x \ y

    val residuals = y - (x * beta)
    val ssResiduals = residuals.t * residuals

    // calculate sum of squared deviations
    val deviations = y - mean(y)
    val ssDeviations = deviations.t * deviations

    // compute the regression parameters standard errors
    val betaVariance = diag(inv(x.t * x))
    val sigma = residuals.t * residuals / (x.rows - x.cols)
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
    val residualDegreesOfFreedom = x.rows - x.cols
    val tDist = StudentsT(residualDegreesOfFreedom)
    val pValue = 2 * tDist.cdf(-math.abs(t))

    LinearAssociation(
      ssDeviations,
      ssResiduals,
      genoSE,
      t,
      residualDegreesOfFreedom,
      pValue,
      beta.data.toList,
      genotypes.numValidSamples)
  }

  private[algorithms] def prepareDesignMatrix(genotypes: CalledVariant,
                                              phenotypes: Map[String, Phenotype],
                                              allelicAssumption: String): (DenseMatrix[Double], DenseVector[Double]) = {

    val validGenos = genotypes.samples.filter(genotypeState => !genotypeState.value.contains(".") && phenotypes.contains(genotypeState.sampleID))

    val samplesGenotypes = allelicAssumption.toUpperCase match {
      case "ADDITIVE"  => validGenos.map(genotypeState => (genotypeState.sampleID, genotypeState.additive))
      case "DOMINANT"  => validGenos.map(genotypeState => (genotypeState.sampleID, genotypeState.dominant))
      case "RECESSIVE" => validGenos.map(genotypeState => (genotypeState.sampleID, genotypeState.recessive))
    }

    val (primitiveX, primitiveY) = samplesGenotypes.flatMap({
      case (sampleID, genotype) if phenotypes.contains(sampleID) => {
        Some(1.0 +: genotype +: phenotypes(sampleID).covariates.toArray, phenotypes(sampleID).phenotype)
      }
      case _ => None
    }).toArray.unzip

    if (primitiveX.length == 0) {
      // TODO: Determine what to do when the design matrix is empty (i.e. no overlap btwn geno and pheno sampleIDs, etc.)
      throw new IllegalArgumentException("No overlap between phenotype and genotype state sample IDs.")
    }

    // NOTE: This may cause problems in the future depending on JVM max varargs, use one of these instead if it breaks:
    // val x = new DenseMatrix(x(0).length, x.length, x.flatten).t
    // val x = new DenseMatrix(x.length, x(0).length, x.flatten, 0, x(0).length, isTranspose = true)
    // val x = new DenseMatrix(x :_*)

    (new DenseMatrix(primitiveX.length, primitiveX(0).length, primitiveX.transpose.flatten), new DenseVector(primitiveY))
  }

  def constructVM(variant: CalledVariant,
                  phenotype: Phenotype,
                  association: LinearAssociation,
                  allelicAssumption: String): LinearVariantModel = {
    LinearVariantModel(variant.uniqueID,
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

object LinearSiteRegression extends LinearSiteRegression {
  val regressionName = "LinearSiteRegression"
}