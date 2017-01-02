/**
 * Copyright 2016 Taner Dagdelen and Frank Nothaft
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

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

trait VariantModel {
  var variantID: String
  var variant: Variant
  var phenotype: String
  var numSamples: Int
  var variance: Double
  var modelType: String // e.g. Additive Logistic, Dominant Linear, etc.
  var hyperParamValues: Map[String, Double]
  var weights: Array[Double]
  var intercept: Double
  var haplotypeBlock: String
  var incrementalUpdateValue: Double
  var QRFactorizationValue: Double
  var QRFactorizationWeights: Array[Double]
  var predictions: List[(Array[(String, (Double, Double))], Association)]
  var association: Association

  def setVariantID(id: String): this.type = {
    variantID = id
    this
  }

  def setPhenotype(pheno: String): this.type = {
    phenotype = pheno
    this
  }

  def setNumSamples(num: Int): this.type = {
    numSamples = num
    this
  }

  def setVariance(vari: Double): this.type = {
    variance = vari
    this
  }

  def setHyperParamValues(hyp: Map[String, Double]): this.type = {
    hyperParamValues = hyp
    this
  }

  def setWeights(w: Array[Double]): this.type = {
    weights = w
    this
  }

  def setIntercept(inter: Double): this.type = {
    intercept = inter
    this
  }

  def setHaplotypeBlock(block: String): this.type = {
    haplotypeBlock = block
    this
  }

  def setIncrementalUpdateValue(value: Double): this.type = {
    incrementalUpdateValue = value
    this
  }
  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): Unit

  def predict(obs: Array[(Double, Array[Double])]): List[(Array[(String, (Double, Double))], Association)]

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  //  def test(observations: Array[(Double, Array[Double])]): Double = {
  //    var correct = 0
  //    var affected = 0
  //    for (elem <- observations) {
  //      val (str, (prediction, actual)): (String, (Double, Double)) = elem
  //      if (prediction == actual) {
  //        correct += 1
  //      }
  //      if (prediction == 1) {
  //        affected += 1
  //      }
  //    }
  //  }

}
