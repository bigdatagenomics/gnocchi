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

import net.fnothaft.gnocchi.transformations.Obs2LabeledPoints
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.optimization.LeastSquaresGradient
import org.bdgenomics.formats.avro.Variant

trait LinearVariantModel extends VariantModel {

  var variance = 0.0
  var variantID = "No ID for this Variant"
  var variant = new Variant
  var hyperParamValues = Map[String, Double]()
  var weights = Array[Double]()
  var intercept = 0.0
  var QRFactorizationWeights = Array[Double]()
  var haplotypeBlock = "Nonexistent HaplotypeBlock"
  var incrementalUpdateValue = 0.0
  var QRFactorizationValue = 0.0
  var numSamples = 0
  var phenotype = "Empty Phenotype"
  var predictions = List[(Array[(String, (Double, Double))], Association)]() //Array[(String, (Double, Double))]()
  var association = Association(variant, phenotype, 0.0, Map())
  //  def update(observations: Array[(Double, Array[Double])],
  //             locus: ReferenceRegion,
  //             altAllele: String,
  //             phenotype: String): Unit = {
  //
  //    val clippedObs = BuildAdditiveLogisticVariantModel.arrayClipOrKeepState(observations)
  //    val assoc = AdditiveLogisticAssociation.regressSite(clippedObs, locus, altAllele, phenotype)
  //    assoc.statistics = assoc.statistics + ("numSamples" -> observations.length)
  //    val numNewSamples = observations.length
  //    val totalSamples = numSamples + numNewSamples
  //    val oldWeight = numSamples.toDouble / totalSamples.toDouble
  //    val newWeight = 1.0 + oldWeight
  //    val newWeights = BreezeDense(assoc.statistics("weights").asInstanceOf[Array[Double]])
  //    weights = (oldWeight * BreezeDense(weights) + newWeight * BreezeDense(newWeights)).toArray
  //    numSamples = totalSamples
  //
  //  }

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  //  def update(observations: Array[(Double, Array[Double])]): Unit = {
  //    println("\n\n\n\n\n\n\n\n\n" + weights.toList + "\n\n\n\n\n\n\n")
  //    // Update the weights
  //    val logGrad = new LogisticGradient(2)
  //    var points = Obs2LabeledPoints(observations)
  //    var cumGrad = new DenseVector(Array.fill[Double](weights.length)(0.0))
  //    points = points //++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points ++ points
  //
  //
  //    for (lp <- points) {
  //      val nu = 1.0 / Math.sqrt(numSamples)
  //      val linear = -1.0 * BreezeDense(weights) dot BreezeDense(lp.features.toArray)
  //      val yhat = 1.0 / (1.0 + Math.exp(linear))
  //      val multiplier = lp.label - yhat
  //      val update = BreezeDense(weights) * multiplier
  //      weights = (BreezeDense(weights) + nu * update).toArray
  //      println("linear : " + linear)
  //      println("yhat : " + yhat)
  //      println("multiplier: "
  //      val features = lp.features
  //      val label = lp.label
  //      val weightsVector = new DenseVector(weights)
  //      val cumGrad = new DenseVector(Array.fill[Double](weights.length)(0.0))
  //      val loss = logGrad.compute(features, label, weightsVector, cumGrad)
  //      val grad = cumGrad
  //      val mu = 1.0 / Math.sqrt(numSamples)
  //      val breezeVector = new breeze.linalg.DenseVector(weights)
  //      weights = (breezeVector - mu * breeze.linalg.DenseVector(grad.toArray)).toArray
  //      println("features: " + features.toArray.toList)
  //      println("label: " + label)
  //      println("weightsVector: " + weightsVector.toArray.toList)
  //      println("grad, loss: " + grad + ", " + loss)
  //      println("mu: " + mu)
  //      println("breezeVector: " + breezeVector.toArray.toList)
  //      println("weights: " + weights.toList)

  //

  //      val normFeatures = normalize(features.toArray)

  //      println("features : " + normFeatures.toList)
  //      println("weights : " + weights.toList)
  //      println("loss : " + loss)
  //      val bDense = BreezeDense(weights)
  //      val featDense = BreezeDense(features.toArray)
  //      val yhat = 1.0 / (1.0 + Math.exp(-(featDense dot bDense)))
  //      println(featDense dot bDense)
  //      println("yhat: " + yhat)
  //            println("spark grad: " + grad.toArray.toList)
  //      println("features: " + features.toArray.toList)
  //      println("label: " + label)
  //      println("weightsVector: " + weightsVector)
  //      println("homemade grad: " + ((label - yhat) * yhat * (1 - yhat) * featDense).toArray.toList)
  //      println("cumGrad : " + cumGrad)
  //      val miniBatchGrad = logGrad.compute(features, label, weightsVector, cumGrad)
  //      weights = (bDense + mu * (label - yhat) * yhat * (1 - yhat) * featDense).toArray

  //      cumGrad += logGrad.compute(features, label, weightsVector, cumGrad)
  // update numSamples other parameters
  //      numSamples += 1
  //    }
  //    println(weights)
  //    weights = (BreezeDense(weights) + mu * BreezeDense(cumGrad.toArray)).toArray

  // TODO: need to update the variance as well.
  // var variance = 0.0
  //  }

  def normalize(features: Array[Double]): Array[Double] = {
    var sm = 0.0
    for (elem <- features) {
      sm += Math.pow(elem, 2)
    }
    val mag = Math.pow(sm, 0.5)
    features.map(elem => elem / mag)
  }

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  //  def predict(observations: Array[(Double, Array[Double])],
  //              locus: ReferenceRegion,
  //              altAllele: String,
  //              phenotype: String): Map[String, Double]
  def predict(obs: Array[(Double, Array[Double])]): List[(Array[(String, (Double, Double))], Association)] = {
    null
  }

  // observations is an array of tuples with (genotypeState, array of phenotypes) where the array of phenotypes has
  // the primary phenotype as the first value and covariates following it.
  //  def test(observations: Array[(Double, Array[Double])],
  //           locus: ReferenceRegion,
  //           altAllele: String,
  //           phenotype: String): Double

}