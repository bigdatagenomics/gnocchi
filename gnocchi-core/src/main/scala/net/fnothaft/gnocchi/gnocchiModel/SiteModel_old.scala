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

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.models.{GenotypeState, Phenotype, Prediction, TestResult}
import org.apache.spark.rdd.RDD

trait SiteModel extends Serializable {


  val latestTR: TestResult
  val numSamples: Double
  val variables: Array[String]
  val description: String
  var parameters: DenseVector[Double]
  val parameterDescriptions: Array[String]

  def update(description: String, x: GenotypeState, y: Phenotype[Array[Double]]): Unit = {

  }

  def batchUpdate(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): Unit = {
    //for each sample in x, update the model
  }

  def recompute(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): Unit = {
    //
  }


  def predict1(x: GenotypeState, y: Phenotype[Array[Double]]): Prediction

  def batchPredict(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): RDD[Prediction]

  def evaluate(preds: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): TestResult

  def crossValidate(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): TestResult

  def applyToEachSite(mlStep () => Unit)

  def saveSite()

  
