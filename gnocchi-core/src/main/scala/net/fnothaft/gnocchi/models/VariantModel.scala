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

trait VariantModel[T] {
	val variantID: String
  val numSamples: Int
  val variance: Double
  val modelType: String // e.g. Additive Logistic, Dominant Linear, etc.
  val hyperParamValues: Map[String, Double]
  val weights: Array[Double]
  val haplotypeBlock: String
  val incrementalUpdateValue: Double
  val QRFactorizationValue: Double


  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  def update(observations: Array[(Double, Array[Double])])


  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  def predict(observations: Array[(Double, Array[Double])],
              locus: ReferenceRegion,
              altAllele: String,
              phenotype: String): Map[String, Double]


  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  def test(observations: Array[(Double, Array[Double])],
           locus: ReferenceRegion,
           altAllele: String,
           phenotype: String): Double

}
