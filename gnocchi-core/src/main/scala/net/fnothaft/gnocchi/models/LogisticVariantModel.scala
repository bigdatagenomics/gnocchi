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

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.association.AdditiveLogisticAssociation
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticVariantModel
import net.fnothaft.gnocchi.transformations.Obs2LabeledPoints
import org.apache.spark.mllib.optimization.LogisticGradient
//import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

trait LogisticVariantModel extends VariantModel {

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
  //    def update(observations: Array[(Double, Array[Double])]): Unit = {

  //  }

  def predict(obs: Array[(Double, Array[Double])]): List[(Array[(String, (Double, Double))], Association)] = {
    // transform the data in to design matrix and y matrix compatible with mllib's logistic regresion
    val observationLength = obs(0)._2.length
    val numObservations = obs.length
    val lp = new Array[LabeledPoint](numObservations)

    val sampleObservations = obs.map(kv => {
      val (geno, pheno) = kv
      val str = "sampleId"
      (geno, pheno, str)
    })

    // iterate over observations, copying correct elements into sample array and filling the x matrix.
    // the first element of each sample in x is the coded genotype and the rest are the covariates.
    var features = new Array[Double](observationLength)
    val samples = new Array[String](sampleObservations.length)
    for (i <- sampleObservations.indices) {
      // rearrange variables into label and features
      features = new Array[Double](observationLength)
      features(0) = sampleObservations(i)._1.toDouble
      sampleObservations(i)._2.slice(1, observationLength).copyToArray(features, 1)
      val label = sampleObservations(i)._2(0)

      // pack up info into LabeledPoint object
      lp(i) = new LabeledPoint(label, new org.apache.spark.mllib.linalg.DenseVector(features))

      samples(i) = sampleObservations(i)._3
    }

    val b = weights

    // TODO: Check that this actually matches the samples with the right results.
    // receive 0/1 results from datapoints and model
    val results = predictLP(lp, b)
    val preds = List((samples zip results, this.association))
    this.predictions = preds
    preds
  }

  def predictLP(lpArray: Array[LabeledPoint], b: Array[Double]): Array[(Double, Double)] = {
    val expitResults = expit(lpArray, b)
    // (Predicted, Actual)
    val predictions = new Array[(Double, Double)](expitResults.length)
    for (j <- predictions.indices) {
      predictions(j) = (lpArray(j).label, Math.round(expitResults(j)))
      //      predictions(j) = (lpArray(j).label, expitResults(j))
    }
    predictions
  }

  def expit(lpArray: Array[LabeledPoint], b: Array[Double]): Array[Double] = {
    val expitResults = new Array[Double](lpArray.length)
    val bDense = DenseVector(b)
    for (j <- expitResults.indices) {
      val lp = lpArray(j)
      expitResults(j) = 1 / (1 + Math.exp(-DenseVector(1.0 +: lp.features.toArray) dot bDense))
    }
    expitResults
  }

  // println("\n\n\n\n\n\n\n\n\n" + weights.toList + "\n\n\n\n\n\n\n")
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

}