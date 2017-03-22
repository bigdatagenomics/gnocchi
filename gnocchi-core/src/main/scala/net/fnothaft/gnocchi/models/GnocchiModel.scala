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

import net.fnothaft.gnocchi.gnocchiModel.{BuildAdditiveLinearVariantModel, BuildAdditiveLogisticVariantModel}
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import net.fnothaft.gnocchi.transformations.{Gs2variant, PairSamplesWithPhenotypes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, Variant}

trait GnocchiModel extends Serializable {
  val metaData: GnocchiModelMetaData
  val variantModels: RDD[VariantModel]
  // the variant model and the observations that the model has been trained on
  val comparisonVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])]

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
   *       metaData.HBDThreshold, then all variants for each haplotype
   *       block are flagged for re-compute.
   * @param rdd RDD of GenotypeState objects from new batch of data.
   * @param phenotypes RDD of Phenotype objects from new batch of data.
   * @param sc SparkContext in which Gnocchi is running.
   */
  def update(rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype],
             sc: SparkContext): Unit = {

    val newData = generateObservations(rdd, phenotypes)

    // Combine the new sample observations with the old observations in comparison VariantModel RDD
    val joinedComparisonData = joinComparisonData(comparisonVariantModels, newData)

    // compute the regressions for the comparison VariantModels
    val updatedComparisonVariantModels = joinedComparisonData.map(kv => {
      val (varModel, obs) = kv
      (buildVariantModel(varModel, obs), obs)
    })

    // join new data with correct VariantModel
    val variantModels = this.variantModels
    val variantsAndModels = variantModels.keyBy(_.variant)
    val vmAndDataRDD = variantsAndModels.join(newData)

    // update all variants with new data
    val updatedVMRdd = vmAndDataRDD.map(kvv => {
      val (variant, (model, data)) = kvv
      model.update(data, new ReferenceRegion(variant.getContig.getContigName, variant.getStart, variant.getEnd), variant.getAlternateAllele, metaData.phenotype)
      (variant, model)
    })
    println("Size of updatedVMRdd = " + updatedVMRdd.count)

    // pair up the QR factorization and incrementalUpdate versions of the selected variantModels
    val comparisonModels = updatedComparisonVariantModels.map(elem => {
      val (varModel, obs) = elem
      varModel
    })
    val incrementalVsComparison = updatedVMRdd.join(comparisonModels.keyBy(_.variant))

    // TODO: compare incremental and comparison models to flag discordant variant models.

    // update metaData
    val updatedMetaData = updateMetaData(numAdditionalSamples, newFlaggedVariantModels)
  }

  /**
    * Generates an RDD of observations that can be used to create or update VariantModels.
    *
    * @param genotypes RDD of GenotypeState objects
    * @param phenotypes RDD of Pheontype objects
    * @return Returns an RDD of arrays of observations (genotype + phenotype), keyed by variant
    */
  def generateObservations(genotypes: RDD[GenotypeState],
                           phenotypes: RDD[Phenotype]): RDD[(Variant, Array[(Double, Array[Double])])] = {
    // convert genotypes and phenotypes into observations
    val data = PairSamplesWithPhenotypes(genotypes, phenotypes)
    // data is RDD[((Variant, String), Iterable[(String, (GenotypeState,Phenotype))])]
    data.map(kvv => {
      val (varStr, genPhenItr) = kvv
      val (variant, phenoName) = varStr
      //.asInstanceOf[Array[(String, (GenotypeState, Phenotype[Array[Double]]))]]
      val obs = genPhenItr.map(gp => {
        val (str, (gs, pheno)) = gp
        val ob = (clipOrKeepState(gs), pheno.value)
        ob
      }).toArray
      (variant, obs)
    })
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
  def joinComparisonData(comparisonVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])],
                         newData: RDD[(Variant, Array[(Double, Array[Double])])]): RDD[(Variant, Array[(Double, Array[Double])])] = {
    val compModels = comparisonVariantModels.map(kv => {
      val (varModel, obs) = kv
      (varModel.variant, (varModel, obs))
    })
    compModels.join(newData)
      .map(kvv => {
        val (variant, (oldData, newData)) = kvv
        val (varModel, obs) = oldData
        val observs = (obs.toList ::: newData.toList).asInstanceOf[Array[(Double, Array[Double])]]
        (varModel, observs): (VariantModel, Array[(Double, Array[Double])])
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
    * Returns all the VariantModel objects associated with the given haplotype block
    * in the variantModels RDD.
    *
    * @param haplotypeBlockId Id of the haplotype block from which to pull
    *                         variant models from the variantModels RDD
    * @return RDD of the VariantModel objects from the variantModels RDD
    *         which are associated with the provided haplotype
    *         block in the GnocchiModel
    */
  def getVariantsFromHaplotypeBlock(haplotypeBlockId: String): RDD[VariantModel] = {
    variantModels.filter(_.haplotypeBlock == haplotypeBlockId)
  }

  /**
    * Returns all the VariantModel objects associated with the given haplotype block
    * in the comparisonVariantModels RDD.
    *
    * @param haplotypeBlockId Id of the haplotype block from which to pull
    *                         variant models from the comparisonVariantModels RDD
    * @return RDD of the VariantModel objects from the comparisonVariantModels RDD
    *         which are associated with the provided haplotype
    *         block in the GnocchiModel
    */
  def getComparisonVariantsFromHaplotypeBlock(haplotypeBlockId: String): RDD[VariantModel] = {
    variantModels.filter(_.haplotypeBlock == haplotypeBlockId)
  }

  /**
    * Saves Gnocchi model by saving GnocchiModelMetaData as Java object,
    * variantModels as parquet, and comparisonVariantModels as parquet.
    */
  def save: Unit = {

  }

  def clipOrKeepState(gs: GenotypeState): Double

  // calls the appropriate version of BuildVariantModel
  def buildVariantModel(varModel: VariantModel,
                        obs: Array[(Double, Array[Double])]): VariantModel

}

trait Additive {

  def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait Dominant {

  def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}

class GnocchiModelMetaData(val numSamples: Int,
                           val haplotypeBlockErrorThreshold: Double,
                           val modelType: String,
                           val variables: String,
                           val flaggedVariantModels: List[String],
                           val phenotype: String) extends Serializable


