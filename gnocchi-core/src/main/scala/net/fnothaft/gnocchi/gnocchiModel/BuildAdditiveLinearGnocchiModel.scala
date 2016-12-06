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
package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.association.AdditiveLinearAssociation
import net.fnothaft.gnocchi.models._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }

/* trait for building a GnocchiModel that has an additive, linear VariantModel at each site (via QR factorization). */
trait BuildAdditiveLinearGnocchiModel extends BuildGnocchiModel {

  /* uses QR factorization to compute an additive linear association at each variant*/
  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    AdditiveLinearAssociation(rdd, phenotypes)
  }

  /* Constructs the GnocchiModel using the weights from the associations produced by fit()*/
  def extractModel(assocs: RDD[Association]): GnocchiModel = {
    //    val numSamples: RDD[(String, Int)] = assocs.count //(VariantID, NumSamples)
    //    val numVariants: Int
    //    val variances: RDD[(String, Double)] // (VariantID, variance)
    //    val haplotypeBlockDeltas: Map[String, Double]
    //    val HBDThreshold: Double // threshold for the haplotype block deltas
    //    val modelType: String // Additive Logistic, Dominant Linear, etc.
    //    val hyperparameterVal: Map[String, Double]
    //    val Description: String
    //    //  val latestTestResult: GMTestResult
    //    val variables: String // name of the phenotype and covariates used in the model
    //    val dates: String // dates of creation and update of each model
    //    val sampleIds: Array[String] // list of sampleIDs from all samples the model has seen.
    //    val variantModels: RDD[(String, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
    //    val qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])] // the variant model and the observations that the model must be trained on
    new AdditiveLogisticGnocchiModel
  }
}

//object BuildAdditiveLinearGnocchiModel extends BuildAdditiveLinear {
//  val regressionName = "Additive Linear Regression with SGD"
//}