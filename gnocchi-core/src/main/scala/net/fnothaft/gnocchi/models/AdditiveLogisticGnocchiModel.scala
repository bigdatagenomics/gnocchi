/**
 * Copyright 2016 Taner Dagdelen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticVariantModel
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

case class AdditiveLogisticGnocchiModel extends Serializable with GnocchiModel with Additive {

  var numSamples = List(("Empty List", 0)) //(VariantID, NumSamples)
  var numVariants = 1
  var variances = List(("Empty List", 0.0)) // (VariantID, variance)
  var haplotypeBlockDeltas = Map[String, Double]()
  var HBDThreshold = 0.0 // threshold for the haplotype block deltas
  var modelType = "Additive Logistic Model" // Additive Logistic, Dominant Linear, etc.
  var hyperparameterVal = Map[String, Double]()
  var Description = "empty description"
  //  val latestTestResult: GMTestResult
  var variables = "Empty list of variable names" // name of the phenotype and covariates used in the model
  var dates = "Dates?" // dates of creation and update of each model
  var sampleIds = List("Empty String", "") // list of sampleIDs from all samples the model has seen.
  var variantModels = List[(Variant, VariantModel)]() //RDD[VariantModel.variantId, VariantModel[T]]
  var qrVariantModels = List[(VariantModel, Array[(Double, Array[Double])])]() // the variant model and the observations that the model must be trained on
  var flaggedVariants = List[Variant]()
  //  val numSamples: RDD[(String, Int)] //(VariantID, NumSamples)
  //  val numVariants: Int
  //  val variances: RDD[(String, Double)] // (VariantID, variance)
  //  val haplotypeBlockDeltas: Map[String, Double]
  //  val HBDThreshold: Double // threshold for the haplotype block deltas
  //  val modelType: String // Additive Logistic, Dominant Linear, etc.
  //  val hyperparameterVal: Map[String, Double]
  //  val Description: String
  //  //  val latestTestResult: GMTestResult
  //  val variables: String // name of the phenotype and covariates used in the model
  //  val dates: String // dates of creation and update of each model
  //  val sampleIds: Array[String] // list of sampleIDs from all samples the model has seen.
  //  var variantModels: RDD[(Variant, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
  //  val qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])] // the variant model and the observations that the model must be trained on

  // filters out all variants that don't pass a certian predicate and returns a GnocchiModel containing only those variants.
  //  def filter: GnocchiModel

  // given a batch of data, update the GnocchiModel with the data (by updating all the VariantModels).
  // Suggest recompute when haplotypeBlockDelta is bigger than some threshold.

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample.
  //  def predict(rdd: RDD[GenotypeState],
  //              phenotypes: RDD[Phenotype[Array[Double]]])

  // calls the appropriate version of BuildVariantModel
  def buildVariantModel(varModel: VariantModel,
                        obs: Array[(Double, Array[Double])]): VariantModel = {
    val variant = varModel.variant
    val phenotype = varModel.phenotype
    BuildAdditiveLogisticVariantModel(obs, variant, phenotype)
  }

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample and comparing to actual value
  //  def test(rdd: RDD[GenotypeState],
  //           phenotypes: RDD[Phenotype[Array[Double]]]): GMTestResult

  // save the model
  //  def save

}