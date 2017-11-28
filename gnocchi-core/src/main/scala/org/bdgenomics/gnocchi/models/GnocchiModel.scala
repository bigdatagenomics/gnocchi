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
package org.bdgenomics.gnocchi.models

import java.io.{ File, PrintWriter }

import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, VariantModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.apache.spark.sql.Dataset

import java.io.FileOutputStream
import java.io.ObjectOutputStream

case class GnocchiModelMetaData(modelType: String,
                                phenotype: String,
                                covariates: String,
                                numSamples: Int,
                                haplotypeBlockErrorThreshold: Double = 0.1,
                                flaggedVariantModels: Option[List[String]] = None) {

  def save(saveTo: String): Unit = {
    val fos = new FileOutputStream(saveTo)
    val oos = new ObjectOutputStream(fos)

    oos.writeObject(this)
    oos.close
  }
}

/**
 * A trait that wraps an RDD of variant-specific models that are incrementally
 * updated, an RDD of variant-specific models that are recomputed over entire
 * sample set (for quality control of incrementally updated models), and
 * metadata.
 *
 * @tparam VM The type of VariantModel in the RDDs of variant-specific models
 * @tparam GM The type of this GnocchiModel
 */
trait GnocchiModel[VM <: VariantModel[VM], GM <: GnocchiModel[VM, GM]] {

  /**
   * Metadata for the model
   */
  val metaData: GnocchiModelMetaData

  /**
   * The RDD of variant-specific models contained in the model
   */
  val variantModels: Dataset[VM]

  /**
   * The RDD of variant-specific models and accompanying data that
   * is to be used to check for departure of incrementally updated
   * models from complete recompute models
   */
  val QCVariantModels: Dataset[QualityControlVariantModel[VM]]

  val QCPhenotypes: Map[String, Phenotype]

  /**
   * Updates a GnocchiModel with new batch of data by updating all the
   * VariantModels
   *
   * @note The genotype and phenotype data from at least one variant in
   *       each haplotype block is saved to the GnocchiModel. With each
   *       update, a full re-compute over all samples for those variants
   *       is performed and the results are compared to the results
   *       from the incrementally updated models for those variants.
   *       If the average (across variants in the haplotype block)
   *       difference in the weight associated with the
   *       genotype parameter in the full re-compute models and that
   *       of the incrementally updated models is greater than
   *       metaData.haplotypeBlockErrorThreshold, then all variants for that haplotype
   *       block are flagged for re-compute.
   * @param newObservations RDD of tuples containing genotype and phenotype data for each
   *                        variant. Format is (variant, array) where array is an Array of
   *                        tuples containing gentoype and phenotype data where the first
   *                        element of the tuple is the genotype state [0, 1, 2]. The
   *                        second element is an array of phenotype values, the frist
   *                        element corresponding to the primary phenotype being
   *                        regressed on, and the remainder corresponding to the covariates.
   */
  def mergeGnocchiModel(otherModel: GnocchiModel[VM, GM]): GnocchiModel[VM, GM]

  def getVariantModels: Dataset[VM] = { variantModels }

  /**
   * Incrementally updates variant models using new batch of data
   *
   * @param newObservations RDD of variants and their associated genotype and phenotype
   *                        data to be used to updated the model for each variant
   * @return Returns an RDD of incrementally updated VariantModels
   */
  def mergeVariantModels(newVariantModels: Dataset[VM]): Dataset[VM]
  // = {
  //    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID")).map(x => x._1.mergeWith(x._2))
  //  }

  //  /**
  //   * Returns VariantModels created from full recompute over all data for each variant
  //   * as well as array containing all phenotype and genotype data for that variant.
  //   *
  //   * @param newObservations New data to be added to existing data for recompute
  //   * @return RDD of VariantModels and associated genotype and phenotype data
  //   */
  //  def mergeQualityControlVariantModels(newQCVariants: Dataset[QualityControlVariantModel[VM]]): Dataset[QualityControlVariantModel[VM]]
  //
  //  /**
  //   * Compares incrementally updated and full-recompute models store in the GnocchiModel in
  //   * order to flag variants for which the pValue differs more than
  //   * haplotypeBlockErrorThreshold between the incrementally-updated
  //   * and full-recompute models.
  //   *
  //   * @param variantModels RDD of variant models
  //   * @param comparisonVariantModels RDD of full-recompute variant models and their
  //   *                                associated data
  //   * @return Returns a list of flagged variants.
  //   */
  //  def compareModels(variantModels: Dataset[VM],
  //                    comparisonVariantModels: Dataset[QualityControlVariantModel[VM]]): Unit = {
  //    // ToDo: Implement!
  //    // pair up the QR factorization and incrementalUpdate versions of the selected variantModels
  //    //    val comparisonModels = comparisonVariantModels.map(elem => {
  //    //      val (varModel, obs) = elem
  //    //      varModel
  //    //    })
  //    //    val incrementalVsComparison = variantModels.keyBy(_.variant)
  //    //      .join(comparisonModels.keyBy(_.variant))
  //    //
  //    //    val comparisons = incrementalVsComparison.map(elem => {
  //    //      val (variant, (variantModel, comparisonVariantModel)) = elem
  //    //      (variantModel.variantId,
  //    //        math.abs(variantModel.pValue - comparisonVariantModel.pValue))
  //    //    })
  //    //    comparisons.filter(elem => {
  //    //      val (variantId, pValueDifference) = elem
  //    //      pValueDifference >= metaData.haplotypeBlockErrorThreshold
  //    //    }).map(p => p._1).collect.toList
  //  }

  /**
   * Returns new GnocchiModelMetaData object with all fields copied except
   * numSamples and flaggedVariantModels updated.
   *
   * @param numAdditionalSamples Number of samples in update data
   * @param newFlaggedVariantModels VariantModels flagged after update
   */
  def updateMetaData(numAdditionalSamples: Int,
                     newFlaggedVariantModels: Option[List[String]] = None): GnocchiModelMetaData = {
    val numSamples = this.metaData.numSamples + numAdditionalSamples

    GnocchiModelMetaData(
      this.metaData.modelType,
      this.metaData.phenotype,
      this.metaData.covariates,
      numSamples,
      this.metaData.haplotypeBlockErrorThreshold,
      if (newFlaggedVariantModels.isDefined) newFlaggedVariantModels else this.metaData.flaggedVariantModels)
  }
  //
  //  def updateQCVariantModels(): Dataset[QualityControlVariantModel[VM]]

  /**
   * Saves Gnocchi model by saving GnocchiModelMetaData as Java object,
   * variantModels as parquet, and comparisonVariantModels as parquet.
   */
  def save(saveTo: String): Unit = {
    variantModels.write.parquet(saveTo + "/variantModels")
    QCVariantModels.write.parquet(saveTo + "/qcModels")
    metaData.save(saveTo + "/metaData")

    val fos = new FileOutputStream(saveTo + "/qcPhenotypes")
    val oos = new ObjectOutputStream(fos)

    oos.writeObject(QCPhenotypes)
    oos.close
  }
}
