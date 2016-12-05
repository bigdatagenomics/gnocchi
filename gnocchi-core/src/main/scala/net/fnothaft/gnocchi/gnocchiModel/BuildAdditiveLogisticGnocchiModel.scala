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

import net.fnothaft.gnocchi.association.AdditiveLogisticAssociation
import net.fnothaft.gnocchi.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant

/* trait for building a GnocchiModel that has an additive, logistic VariantModel at each site (via QR factorization). */
trait BuildAdditiveLogistic extends BuildGnocchiModel {

  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    AdditiveLogisticAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association], sc: SparkContext): GnocchiModel = {
    //code for packaging up the association object elements into a GnocchiModel
    val gm = new AdditiveLogisticGnocchiModel
    gm.variantModels = assocs.map(assoc => {
      val model = BuildAdditiveLogisticVariantModel.extractVariantModel(assoc)
      (model.variant, model)
    })
    val model = new AdditiveLogisticGnocchiModel
    //TODO: make setters for the GnocchiModel fields and set the values in here.

    gm.numSamples: RDD[(String, Int)] = sc.parallelize(List(("Empty", 1))) //(VariantID, NumSamples)
    gm.numVariants: Int = 1
    gm.variances: RDD[(String, Double)] = sc.parallelize(List(("Empty", 1.0))) // (VariantID, variance)
    gm.haplotypeBlockDeltas: Map[String, Double] = Map[String, Double]()
    gm.HBDThreshold: Double = 0.0// threshold for the haplotype block deltas
    gm.modelType: String = "Additive Logistic Model"// Additive Logistic, Dominant Linear, etc.
    gm.hyperparameterVal: Map[String, Double] = Map[String, Double]()
    gm.Description: String = "empty description"
    //  val latestTestResult: GMTestResult
    gm.variables: String  = "Empty list of variable names"// name of the phenotype and covariates used in the model
    gm.dates: String = "Dates?"// dates of creation and update of each model
    gm.sampleIds: Array[String] = Array("Empty String")// list of sampleIDs from all samples the model has seen.
//    var variantModels: RDD[(Variant, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
//    gm.qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])]// the variant model and the observations that the model must be trained on
    gm
  }
}

object BuildAdditiveLogisticGnocchiModel extends BuildAdditiveLogistic {
  val regressionName = "Additive Logistic Regression with SGD"
}