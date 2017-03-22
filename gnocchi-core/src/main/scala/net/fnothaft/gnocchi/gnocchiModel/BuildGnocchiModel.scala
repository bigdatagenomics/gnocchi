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

import net.fnothaft.gnocchi.algorithms.siteregression.{AdditiveLinearAssociation, AdditiveLogisticAssociation, DominantLinearAssociation, DominantLogisticAssociation}
import net.fnothaft.gnocchi.algorithms.{DominantLinearAssociation, DominantLogisticAssociation}
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.linear.AdditiveLinearGnocchiModel
import net.fnothaft.gnocchi.models.logistic.AdditiveLogisticGnocchiModel
import net.fnothaft.gnocchi.rdd.association.Association
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait BuildGnocchiModel {

  def apply(rdd: RDD[GenotypeState],
            phenotypes: RDD[Phenotype],
            sc: SparkContext,
            save: Boolean = false): (GnocchiModel, RDD[Association]) = {

    // call RegressPhenotypes on the data
    val assocs = fit(rdd, phenotypes)

    // extract the model parameters (including p-value) for each variant and build LogisticGnocchiModel
    val model = extractModel(assocs, sc)

    (model, assocs)
  }

  def fit(rdd: RDD[GenotypeState],
          phenotypes: RDD[Phenotype]): RDD[Association]

  def extractModel(assocs: RDD[Association], sc: SparkContext): GnocchiModel

}

trait BuildDominantLogistic extends BuildGnocchiModel {

  def fit(rdd: RDD[GenotypeState],
          phenotypes: RDD[Phenotype]): RDD[Association] = {
    DominantLogisticAssociation(rdd, phenotypes)
  }

  //  def extractModel(assocs: RDD[Association]): GnocchiModel = {
  //
  //    //code for packaging up the association object elements into a GnocchiModel
  //    new GnocchiModel()
  //  }
}

//object BuildDominantLogisticGnocchiModel extends BuildDominantLogistic {
//  val regressionName = "Dominant Logistic Regression with SGD"
//}

trait BuildDominantLinear extends BuildGnocchiModel {

  def fit(rdd: RDD[GenotypeState],
          phenotypes: RDD[Phenotype]): RDD[Association] = {
    DominantLinearAssociation(rdd, phenotypes)
  }

  //  def extractModel(assocs: RDD[Association]): GnocchiModel = {
  //
  //    //code for packaging up the association object elements into a GnocchiModel
  //
  //  }
}

//object BuildDominantLinearGnocchiModel extends BuildDominantLogistic {
//  val regressionName = "Dominant Linear Regression with SGD"
//}

trait BuildAdditiveLogistic extends BuildGnocchiModel {

  def apply(genotypeStates: RDD[GenotypeState],
            phenotypes: RDD[Phenotype],
            sc: SparkContext): (GnocchiModel, RDD[Association]) = {
    val assocs = fit(genotypeStates, phenotypes)
    (extractModel(assocs, sc), assocs)
  }

  def fit(rdd: RDD[GenotypeState],
          phenotypes: RDD[Phenotype]): RDD[Association] = {
    AdditiveLogisticAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association], sc: SparkContext): GnocchiModel = {
    //code for packaging up the association object elements into a GnocchiModel
    val gm = new AdditiveLogisticGnocchiModel
    println(" \n\n\n Associations \n\n\n")
    println(assocs.collect.toList)
    gm.variantModels = assocs.map(assoc => {
      val model = BuildAdditiveLogisticVariantModel.extractVariantModel(assoc)
      // key by variant because there is not legitimate variantId field
      (model.variant, model)
    }).collect.toList
    //TODO: make setters for the GnocchiModel fields and set the values in here.
    //    var variantModels: RDD[(Variant, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
    //    gm.qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])]// the variant model and the observations that the model must be trained on
    gm
  }
}

object BuildAdditiveLogisticGnocchiModel extends BuildAdditiveLogistic {
  val regressionName = "Additive Logistic Regression with Incremental Update"
}

trait BuildAdditiveLinear extends BuildGnocchiModel {

  //  def apply[T](genotypeStates: RDD[GenotypeState],
  //               phenotypes: RDD[Phenotype[T]],
  //               sc: SparkContext): (GnocchiModel, RDD[Association]) = {
  //    val assocs = fit(genotypeStates, phenotypes)
  //    (extractModel(assocs, sc), assocs)
  //  }

  /* uses QR factorization to compute an additive linear association at each variant*/
  def fit(rdd: RDD[GenotypeState],
          phenotypes: RDD[Phenotype]): RDD[Association] = {
    AdditiveLinearAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association], sc: SparkContext): GnocchiModel = {
    //code for packaging up the association object elements into a GnocchiModel
    val gm = new AdditiveLinearGnocchiModel
    gm.variantModels = assocs.map(assoc => {
      val model = BuildAdditiveLinearVariantModel.extractVariantModel(assoc)
      (model.variant, model)
    }).collect.toList
    //TODO: make setters for the GnocchiModel fields and set the values in here.
    //    var variantModels: RDD[(Variant, VariantModel)] //RDD[VariantModel.variantId, VariantModel[T]]
    //    gm.qrVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])]// the variant model and the observations that the model must be trained on
    gm
  }
}

object BuildAdditiveLinearGnocchiModel extends BuildAdditiveLinear {
  val regressionName = "Additive Linear Regression with Incremental Update"
}