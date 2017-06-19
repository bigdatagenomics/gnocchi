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
package net.fnothaft.gnocchi.models

import net.fnothaft.gnocchi.models.variant.VariantModel
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

class GnocchiModelMetaData(val numSamples: Int,
                           val haplotypeBlockErrorThreshold: Double,
                           val modelType: String,
                           val variables: String,
                           val flaggedVariantModels: List[String],
                           val phenotype: String) extends Serializable

case class QualityControlVariant[VM <: VariantModel[VM]](variantModel: VM, observations: Array[(Double, Array[Double])]) extends Serializable

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
  val variantModels: RDD[VM]

  /**
   * The RDD of variant-specific models and accompanying data that
   * is to be used to check for departure of incrementally updated
   * models from complete recompute models
   */
  val comparisonVariantModels: RDD[(VM, Array[(Double, Array[Double])])]

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
  def update[CT](newObservations: RDD[(Variant, Array[(Double, Array[Double])])])(implicit ct: ClassTag[VM]): GM = {

    val updatedVariantModels = updateVariantModels(newObservations)

    val updatedComparisonVariantModels = updateComparisonVariantModels(newObservations)

    val newFlaggedVariantModels = compareModels(updatedVariantModels, updatedComparisonVariantModels)

    // TODO: Get rid of this! Having an action here will cause staging inefficiencies.
    val numAdditionalSamples = newObservations.take(1).head.asInstanceOf[(Variant, Array[(Double, Array[Double])])]
      ._2.length

    val updatedMetaData = updateMetaData(numAdditionalSamples, newFlaggedVariantModels)

    constructGnocchiModel(updatedMetaData, updatedVariantModels,
      updatedComparisonVariantModels)

  }

  /**
   * Incrementally updates variant models using new batch of data
   *
   * @param newObservations RDD of variants and thier associated genotype and phenotype
   *                        data to be used to updated the model for each variant
   * @return Returns an RDD of incrementally updated VariantModels
   */
  def updateVariantModels(newObservations: RDD[(Variant, Array[(Double, Array[Double])])])(implicit ct: ClassTag[VM]): RDD[VM] = {
    // join new data with correct VariantModel
    val vmAndDataRDD = variantModels.keyBy(_.variant).join(newObservations)

    println("SDFSDF")
    println(vmAndDataRDD.map(p => p._2._1.weights).collect.toList)
    // update all variants with new data
    vmAndDataRDD.map(kvv => {
      val (variant, (model, data)) = kvv
      val updatedModel = model.update(data)
      updatedModel
    })
  }

  /**
   * Returns VariantModels created from full recompute over all data for each variant
   * as well as array containing all phenotype and genotype data for that variant.
   *
   * @param newObservations New data to be added to existing data for recompute
   * @return RDD of VariantModels and associated genotype and phenotype data
   */
  def updateComparisonVariantModels(newObservations: RDD[(Variant, Array[(Double, Array[Double])])]): RDD[(VM, Array[(Double, Array[Double])])] = {
    // Combine the new sample observations with the old observations
    val joinedComparisonData = mergeObservations(comparisonVariantModels, newObservations)

    // compute the regressions for the comparison VariantModels
    val updatedComparisonVariantModels = joinedComparisonData.map(kv => {
      val (varModel, obs) = kv
      (regress(obs, varModel.variant, varModel.phenotype, varModel.phaseSetId), obs)
    })
    updatedComparisonVariantModels
  }

  /**
   * Compares incrementally updated and full-recompute models store in the GnocchiModel in
   * order to flag variants for which the pValue differs more than
   * haplotypeBlockErrorThreshold between the incrementally-updated
   * and full-recompute models.
   *
   * @param variantModels RDD of variant models
   * @param comparisonVariantModels RDD of full-recompute variant models and their
   *                                associated data
   * @return Returns a list of flagged variants.
   */
  def compareModels[CT](variantModels: RDD[VM],
                        comparisonVariantModels: RDD[(VM, Array[(Double, Array[Double])])])(implicit ct: ClassTag[VM]): List[String] = {
    // pair up the QR factorization and incrementalUpdate versions of the selected variantModels
    val comparisonModels = comparisonVariantModels.map(elem => {
      val (varModel, obs) = elem
      varModel
    })
    val incrementalVsComparison = variantModels.keyBy(_.variant)
      .join(comparisonModels.keyBy(_.variant))

    val comparisons = incrementalVsComparison.map(elem => {
      val (variant, (variantModel, comparisonVariantModel)) = elem
      (variantModel.variantId,
        math.abs(variantModel.pValue - comparisonVariantModel.pValue))
    })
    comparisons.filter(elem => {
      val (variantId, pValueDifference) = elem
      pValueDifference >= metaData.haplotypeBlockErrorThreshold
    }).map(p => p._1).collect.toList
  }

  /**
   * Combines old data stored with comparison VariantModels, and the new data for update.
   *
   * @param comparisonVariantModels RDD of globally optimized VariantModels with data
   * @param newData New data to be joined with already stored data for globally optimized
   *                VariantModels
   *
   * @return same RDD as comparisonVariantModels, except the data stored with the VariantModels
   *         contains both the old data and the data for update.
   */
  def mergeObservations(comparisonVariantModels: RDD[(VM, Array[(Double, Array[Double])])],
                        newData: RDD[(Variant, Array[(Double, Array[Double])])]): RDD[(VM, Array[(Double, Array[Double])])] = {
    val compModels = comparisonVariantModels.map(kv => {
      val (varModel, obs) = kv
      (varModel.variant, (varModel, obs))
    })
    compModels.join(newData)
      .map(kvv => {
        val (variant, (oldData, newData)) = kvv
        val (varModel, obs) = oldData
        val observs = (obs.toList ::: newData.toList).toArray
        (varModel, observs): (VM, Array[(Double, Array[Double])])
      })
  }

  /**
   * Returns new GnocchiModelMetaData object with all fields copied except
   * numSamples and flaggedVariantModels updated.
   *
   * @param numAdditionalSamples Number of samples in update data
   * @param newFlaggedVariantModels VariantModels flagged after update
   */
  def updateMetaData(numAdditionalSamples: Int, newFlaggedVariantModels: List[String]): GnocchiModelMetaData = {
    val numSamples = this.metaData.numSamples + numAdditionalSamples
    new GnocchiModelMetaData(numSamples,
      this.metaData.haplotypeBlockErrorThreshold,
      this.metaData.modelType,
      this.metaData.variables,
      newFlaggedVariantModels,
      this.metaData.phenotype)
  }

  /**
   * Returns GnocchiModel of same subtype as the calling GnocchiModel
   *
   * @param metaData Metadata for the new GnocchiModel
   * @param variantModels RDD of variant models for the new GnocchiModel
   * @param comparisonVariantModels RDD of comparison variant models and
   *                                accompanying data for the new GnocchiModel
   * @return Returns a new GnochhiModel with the given parameters and
   *         of the same subtype as the GnocchiModel which is calling
   *         constructGnocchiModel
   */
  def constructGnocchiModel(metaData: GnocchiModelMetaData,
                            variantModels: RDD[VM],
                            comparisonVariantModels: RDD[(VM, Array[(Double, Array[Double])])]): GM

  /**
   * Saves Gnocchi model by saving GnocchiModelMetaData as Java object,
   * variantModels as parquet, and comparisonVariantModels as parquet.
   */
  def save(saveTo: String): Unit

  /**
   * Runs a regression on the data for the given variant and returns
   * a variant model of correct subtype, based on subtype of GnocchiModel
   *
   * @param observations Genotype and phenotype data to be used when creating the
   *                     VariantModel
   * @param variant Variant at which the regression is to be run
   * @param phenotype Description of the phenotype and covariate variables.
   * @return Returns the VariantModel (of correct subtype, based on subtype
   *         of the GnocchiModel) which results from the regression
   */
  def regress(observations: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String,
              phaseSetId: Int): VM

}

