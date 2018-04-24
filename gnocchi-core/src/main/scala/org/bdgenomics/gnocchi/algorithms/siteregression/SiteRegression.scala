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

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.VariantModel
import org.bdgenomics.gnocchi.primitives.association.Association
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.utils.misc.Logging

import scala.collection.immutable.Map

trait SiteRegression[VM <: VariantModel[VM], A <: Association] extends Serializable with Logging {

  //  val regressionName: String

  //  def apply(genotypes: Dataset[CalledVariant],
  //            phenotypes: Broadcast[Map[String, Phenotype]],
  //            allelicAssumption: String = "ADDITIVE",
  //            validationStringency: String = "STRICT"): Dataset[VM]
  //
  //  def applyToSite(phenotypes: Map[String, Phenotype],
  //                  genotypes: CalledVariant,
  //                  allelicAssumption: String): A

  //  def constructVM(variant: CalledVariant,
  //                  phenotype: Phenotype,
  //                  association: A,
  //                  allelicAssumption: String): VM

  /**
   * Data preparation function that converts the gnocchi models into breeze linear algebra primitives BLAS/LAPACK
   * optimizations.
   *
   * @todo Move the allelic assumption [[String]] to
   *       [[org.bdgenomics.gnocchi.utils.AllelicAssumption.AllelicAssumption]] type.
   *
   * @param phenotypes [[Phenotype]]s map that contains the labels (primary phenotype) and part of the design matrix
   *                  (covariates)
   * @param genotypes [[CalledVariant]] object to convert into a breeze design matrix
   * @param allelicAssumption [[String]] that denotes what allelic assumption of ADDITIVE / DOMINANT
   *                         / RECESSIVE to use for the data preparation
   * @return tuple where first element is the [[DenseMatrix]] design matrix and second element
   *         is [[DenseVector]] of labels
   */
  def prepareDesignMatrix(genotypes: CalledVariant,
                          phenotypes: Map[String, Phenotype],
                          allelicAssumption: String): (DenseMatrix[Double], DenseVector[Double]) = {

    val validGenos = genotypes.samples.filter { case (id, genotypeState) => genotypeState.misses == 0 && phenotypes.contains(id) }

    val samplesGenotypes = allelicAssumption.toUpperCase match {
      case "ADDITIVE"  => validGenos.map { case (sampleID, genotypeState) => (sampleID, genotypeState.additive) }
      case "DOMINANT"  => validGenos.map { case (sampleID, genotypeState) => (sampleID, genotypeState.dominant) }
      case "RECESSIVE" => validGenos.map { case (sampleID, genotypeState) => (sampleID, genotypeState.recessive) }
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
}

