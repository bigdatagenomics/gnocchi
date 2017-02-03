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

import net.fnothaft.gnocchi.association.DominantLogisticAssociation
import net.fnothaft.gnocchi.models.{ Association, GenotypeState, GnocchiModel, Phenotype }
import org.apache.spark.rdd.RDD

trait BuildDominantLogistic extends BuildGnocchiModel {

  def fit[T](rdd: RDD[GenotypeState],
             phenotypes: RDD[Phenotype[T]]): RDD[Association] = {
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

