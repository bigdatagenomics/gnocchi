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

import org.apache.spark.rdd.RDD

trait GnocchiModel[T] {
  val numSamples: RDD[(String, Int)] //(VariantID, NumSamples)
  val numVariants: Int
  val variances: RDD[(String, Double)] // (VariantID, variance)
  val haplotypeBlockDeltas: Map[String, Double]
  val HBDThreshold: Double // threshold for the haplotype block deltas
  val modelType: String // Additive Logistic, Dominant Linear, etc.
  val hyperparameterVal: Map[String, Double]
  val Description: String
  val latestTestResult: GMTestResult
  val variables: String // name of the phenotype and covariates used in the model
  val dates: String // dates of creation and update of each model
  val sampleIds: Array[String] // list of sampleIDs from all samples the model has seen.
  val variantModels: RDD[VariantModel[T]]
  val qrVariantModels: RDD[(VariantModel[T], Array[(Double, Array[Double])]] // the variant model and the observations that the model must be trained on




  // filters out all variants that don't pass a certian predicate and returns a GnocchiModel containing only those variants.
  def filter: GnocchiModel[T]




  // given a batch of data, update the GnocchiModel with the data (by updating all the VariantModels).
  // Suggest recompute when haplotypeBlockDelta is bigger than some threshold.
  def update(rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): Unit = {

    // combine the new sample observations with the old ones.
//    val qrKeyedByID = qrVariantModels.map(qr => {
//      (qr._1.variantID, qr._2)
//    })
//    rdd.keyBy(_.sampleId)
//      // join together the samples with both genotype and phenotype entry
//      .join(phenotypes.keyBy(_.sampleId))
//      .map(kvv => {
//        // unpack the entry of the joined rdd into id and actual info
//        val (_, p) = kvv
//        // unpack the information into genotype state and pheno
//        val (gs, pheno) = p
//        // extract referenceAllele and phenotype and pack up with p, then group by key
//        (gs.variantID, p)
//      }).groupByKey().join(qrKeyedByID)
//      .map(site => {
//        val (((pos, allele), phenotype), observations) = site


    // recompute using QR factorization on specific variants in each haplotype block

    // convert the genotypes and phenotypes into the format VariantModel can take

    // map an update call to all variants

    // compare the updated weights to the QR Factorization weights and flag Variants for recompute

  }

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample.
  def predict(rdd: RDD[GenotypeState],
              phenotypes: RDD[Phenotype[T]])

  // apply the GnocchiModel to a new batch of samples, predicting the phenotype of the sample and comparing to actual value
  def test(rdd: RDD[GenotypeState],
           phenotypes: RDD[Phenotype[T]]): GMTestResult

  // save the model
  def save

}