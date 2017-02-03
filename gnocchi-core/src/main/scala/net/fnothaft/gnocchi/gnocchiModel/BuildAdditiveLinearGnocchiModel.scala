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
package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.association.AdditiveLinearAssociation
import net.fnothaft.gnocchi.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }

/* trait for building a GnocchiModel that has an additive, linear VariantModel at each site (via QR factorization). */
trait BuildAdditiveLinear extends BuildGnocchiModel {

  /* uses QR factorization to compute an additive linear association at each variant*/
  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    AdditiveLinearAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association], sc: SparkContext): GnocchiModel = {
    //code for packaging up the association object elements into a GnocchiModel
    val gm = new AdditiveLogisticGnocchiModel
    gm.variantModels = assocs.map(assoc => {
      val model = BuildAdditiveLogisticVariantModel.extractVariantModel(assoc)
      (model.variant, model)
    }).collect.toList
    val model = new AdditiveLogisticGnocchiModel
    //TODO: make setters for the GnocchiModel fields and set the values in here.
    //    var variantModels: RDD[(Variant, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
    //    gm.qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])]// the variant model and the observations that the model must be trained on
    gm
  }
}

object BuildAdditiveLinearGnocchiModel extends BuildAdditiveLinear {
  val regressionName = "Additive Linear Regression with SGD"
}