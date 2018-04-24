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

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.utils.ModelType._

/**
 * Data container for [[LinearVariantModel]] and the associated metadata that is shared between
 * the independent variant models in an association study.
 *
 * @param variantModels Dataset of [[LinearVariantModel]], each of which store the variant level
 *                      statistical model for the study
 * @param phenotypeName The names of the primary phenotype for the study being conducted. This
 *                      phenotype acts as the label or Y of a sample when building each variant model.
 * @param covariatesNames The names of the covariates used in for the study being conducted. These
 *                        variable are the variables that are being controlled for, when building each
 *                        variant model
 * @param sampleUIDs The unique identifiers for the subjects used in this study, to build this model
 * @param numSamples the number of subjects used in this study, to build this model
 * @param allelicAssumption The allelic assumption used in this study, to build this model
 */
case class LinearGnocchiModel(@transient variantModels: Dataset[LinearVariantModel],
                              phenotypeName: String,
                              covariatesNames: List[String],
                              sampleUIDs: Set[String],
                              numSamples: Int,
                              allelicAssumption: String)
    extends GnocchiModel[LinearVariantModel, LinearGnocchiModel] {
  val modelType: ModelType = Linear

  import variantModels.sqlContext.implicits._

  /**
   * Merge two [[LinearGnocchiModel]]s together into the union of the two models. This is done by
   * summing each of the constituent [[LinearVariantModel]] xTx matrices pointwise and solving the
   * linear system of equations for the pointwise summed vector or xTy.
   *
   * @param otherModel The second [[LinearGnocchiModel]] to merge into this model
   * @param allowDifferentPhenotype allows for there to be difference in the names of primary
   *                                phenotype or the covariates. Allows for collaborators with
   *                                similar phenotypes that are named differently to collaboratively
   *                                build models
   * @return A merged [[LinearGnocchiModel]] which is equivalent to a model built on the union of
   *         the two seperate datasets
   */
  def mergeGnocchiModel(otherModel: LinearGnocchiModel,
                        allowDifferentPhenotype: Boolean = false): LinearGnocchiModel = {

    require((otherModel.sampleUIDs & sampleUIDs).isEmpty,
      "There are overlapping samples in the datasets used to build the two models.")
    require(allelicAssumption == otherModel.allelicAssumption,
      "The two models are of different allelic assumptions. (additive / dominant / recessive)")
    if (!allowDifferentPhenotype) {
      require(otherModel.phenotypeName == phenotypeName,
        "The two models have different primary phenotypes.")
      require(otherModel.covariatesNames == covariatesNames,
        "The two models have different primary phenotypes.")
    }

    val mergedVMs = mergeVariantModels(otherModel.variantModels)
    // ToDo: Ensure that the same phenotypes and covariates are being used
    LinearGnocchiModel(
      mergedVMs,
      phenotypeName,
      covariatesNames,
      otherModel.sampleUIDs.toSet | sampleUIDs.toSet,
      otherModel.numSamples + numSamples,
      allelicAssumption)
  }

  /**
   * Merge the dataset of [[LinearVariantModel]] by joining the two datasets together and calling
   * the [[LinearVariantModel.mergeWith()]] function on the two variant models
   *
   * @param newVariantModels [[Dataset]] of [[LinearVariantModel]] to merge into the existing set of
   *                        variant models stored in this object.
   * @return the dataset of merged [[LinearVariantModel]]
   */
  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID"))
      .map(x => x._1.mergeWith(x._2))
  }
}
