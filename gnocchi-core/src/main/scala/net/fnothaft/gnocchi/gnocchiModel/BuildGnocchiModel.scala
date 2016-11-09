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

import net.fnothaft.gnocchi.models.{ GenotypeState, Phenotype, Association }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.regression.LabeledPoint
import net.fnothaft.gnocchi.transformations.GP2LabeledPoint

trait BuildGnocchiModel {

  def apply[T](rdd: RDD[GenotypeState],
               phenotypes: RDD[Phenotype[T]]): GnocchiModel = {

    // call RegressPhenotypes on the data
    val assocs = fit(rdd, phenotypes)

    // extract the model parameters (including p-value) for each variant and build LogisticGnocchiModel
    val model = extractModel(assocs)

    // save the LogisticGnocchiModel
    model.save

    model
  }

  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association]

  def extractModel(assocs: RDD[Association]): GnocchiModel

}

trait BuildAdditiveLogistic extends BuildGnocchiModel {

  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    AdditiveLogisticAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association]): GnocchiModel = {

    //code for packaging up the association object elements into a GnocchiModel

  }
}

trait BuildDominantLogistic extends BuildGnocchiModel {



  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    DominantLogisticAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association]): GnocchiModel = {

    //code for packaging up the association object elements into a GnocchiModel

  }
}

trait BuildAdditiveLinear extends BuildGnocchiModel {

  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    AdditiveLinearAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association]): GnocchiModel = {

    //code for packaging up the association object elements into a GnocchiModel

  }
}

trait BuildDominantLinear extends BuildGnocchiModel {



  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
    DominantLinearAssociation(rdd, phenotypes)
  }

  def extractModel(assocs: RDD[Association]): GnocchiModel = {

    //code for packaging up the association object elements into a GnocchiModel

  }
}

object BuildDominantLogisticGnocchiModel extends BuildDominantLogistic {
  val regressionName = "Dominant Logistic Regression with SGD"
}

object BuildAdditiveLogisticGnocchiModel extends BuildAdditiveLogistic {
  val regressionName = "Additive Logistic Regression with SGD"
}

object BuildDominantLinearGnocchiModel extends BuildDominantLogistic {
  val regressionName = "Dominant Linear Regression with SGD"
}

object BuildAdditiveLinearGnocchiModel extends BuildAdditiveLogistic {
  val regressionName = "Additive Linear Regression with SGD"
}