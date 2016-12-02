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

import net.fnothaft.gnocchi.association.AdditiveLogisticAssociation
import net.fnothaft.gnocchi.transformations.Obs2LabeledPoints
import org.apache.spark.mllib.optimization.LogisticGradient
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark.mllib.linalg.DenseVector
import org.bdgenomics.formats.avro.Variant

trait LogisticVariantModel extends VariantModel{

  var variance = 0.0
  var variantID = "No ID for this Variant"
  var variant = new Variant
  var hyperParamValues = Map[String, Double]()
  var weights = Array[Double]()
  var QRFactorizationWeights = Array[Double]()
  var haplotypeBlock = "Nonexistent HaplotypeBlock"
  var incrementalUpdateValue = 0.0
  var QRFactorizationValue = 0.0
  var numSamples = 0

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  def update(observations: Array[(Double, Array[Double])]): Unit = {
    // Update the weights
    val logGrad = new LogisticGradient(2)
    val points = Obs2LabeledPoints(observations)
    val weightsVector = new DenseVector(weights)
    val breezeVector = new breeze.linalg.DenseVector(weights)
    for (lp <- points) {
      val features = lp.features
      val label = lp.label
      weights = (breezeVector - breeze.linalg.DenseVector(logGrad.compute(features, label, weightsVector)._1.toArray)).toArray
      numSamples += 1
    }

    // update numSamples other parameters
    numSamples += observations.length

    // TODO: need to update the variance as well.
    // var variance = 0.0
 }

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
//  def predict(observations: Array[(Double, Array[Double])],
//              locus: ReferenceRegion,
//              altAllele: String,
//              phenotype: String): Map[String, Double]

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
//  def test(observations: Array[(Double, Array[Double])],
//           locus: ReferenceRegion,
//           altAllele: String,
//           phenotype: String): Double

}