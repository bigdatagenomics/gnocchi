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

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.Association
import org.apache.spark.mllib.regression.LabeledPoint
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

class LinearGnocchiModelSuite extends GnocchiFunSuite {

  sparkTest("Build AdditiveLinearGnocchiModel from data and update") {
    // read in the data from binary.csv
    // data comes from: http://www.ats.ucla.edu/stat/sas/dae/binary.sas7bdat
    // results can be found here: http://www.ats.ucla.edu/stat/sas/dae/logit.htm
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows

    // transform it into the right format
    val observation = data.map(row => {
      val geno: Double = row(0)
      val covars: Array[Double] = row.slice(1, 3)
      val phenos: Array[Double] = Array(row(3)) ++ covars
      (geno, phenos)
    })
    val observations = observation.collect
    val altAllele = "No allele"
    val phenotype = "acceptance"
    val locus = ReferenceRegion("Name", 1, 2)
    val scOption = Option(sc)
    val variant = new Variant()
    val contig = new Contig()
    contig.setContigName(locus.referenceName)
    variant.setContig(contig)
    variant.setStart(locus.start)
    variant.setEnd(locus.end)
    variant.setAlternateAllele(altAllele)
    // break observations into initial group and group to use in update 
    val initial = observations.slice(0, 25)
    val forUpdate = observations.slice(25, observations.length)
    val updateGroups = forUpdate.toList.grouped(25).toList

    // feed it into logisitic regression and compare the Wald Chi Squared tests
    val incrementalVariantModel = BuildAdditiveLinearVariantModel(initial, variant, phenotype)
    for (group <- updateGroups) {
      incrementalVariantModel.update(group.toArray, locus, altAllele, phenotype)
    }
    val nonincremental = BuildAdditiveLinearVariantModel(observations, variant, phenotype)

    val incrementalAssoc = Association(incrementalVariantModel.variant, "pheno", 0.0, Map("weights" -> incrementalVariantModel.weights))
    val nonincrementalAssoc = Association(incrementalVariantModel.variant, "pheno", 0.0, Map("weights" -> nonincremental.weights))

    val obs = observation.map(kv => {
      val (geno, pheno) = kv
      val str = "sampleId"
      (geno, pheno, str)
    })

    val incrementalPredictions = predictSite(obs.collect, incrementalAssoc)
    var numCorrectIncre = 0
    var numTotalIncre = incrementalPredictions.length
    for (result <- incrementalPredictions) {
      val (id, (actual, pred)) = result
      println("Incremental actual, pred: " + actual + " " + pred)
      if (actual == pred) {
        numCorrectIncre += 1
      }
    }

    val nonIncrementalPredictions = predictSite(obs.collect, nonincrementalAssoc)
    var numCorrectNonIncre = 0
    var numTotalNonIncre = nonIncrementalPredictions.length
    for (result <- nonIncrementalPredictions) {
      val (id, (actual, pred)) = result
      println("nonIncremental actual, pred: " + actual + " " + pred)
      if (actual == pred) {
        numCorrectNonIncre += 1
      }
    }

    val incrementalAccuracy = numCorrectIncre.toDouble / numTotalIncre
    val nonIncrementalAccuracy = numCorrectNonIncre.toDouble / numTotalNonIncre

    // Assert that the accuracy is close to what we get from using QR method.
    println("non incremental: " + nonIncrementalAccuracy + "\n Weights: " + nonincremental.weights.toList)
    println("incremental: " + incrementalAccuracy + "\n Weights: " + incrementalVariantModel.weights.toList)

    assert(incrementalAccuracy < nonIncrementalAccuracy + .1 && incrementalAccuracy > nonIncrementalAccuracy - .1, "Accuracies do not match up.")

    // Assert that the weights are correct within a threshold.
    //    val estWeights: Array[Double] = variantModel.weights :+ variantModel.intercept
    //    val compWeights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    //    for (i <- 0 until 3) {
    //      assert(estWeights(i) <= (compWeights(i) + 1), s"Weight $i incorrect")
    //      assert(estWeights(i) >= (compWeights(i) - 1), s"Weight $i incorrect")
    //    }
    assert(incrementalVariantModel.numSamples == observations.length, s"NumSamples incorrect: $incrementalVariantModel.numSamples vs $observations.length")
  }

  sparkTest("[NOT IMPLEMENTED YET] Build logistic gnocchi model, update, and check results.") {

    // break data up into initial and update

    // build a bunch of variant models, some with qr, some without different

    // build a logisitic Gnocchi model with the variant models

    // run update on the Gnocchi Model

    // check that the variant's get updated correctly

    // check that the variant's that should have been recomputed using qr did get recomputed using qr

    // check that variant's got flagged when they were supposed to.

  }

  sparkTest("[NOT IMPLEMENTED YET] Load logistic gnocchi model, update, and check results.") {

    // break data up into initial and update

    // build a bunch of variant models, some with qr, some without different

    // build a logisitic Gnocchi model with the variant models

    // save the model

    // load the model

    // run update on the Gnocchi Model

    // check that the variant's get updated correctly

    // check that the variant's that should have been recomputed using qr did get recomputed using qr

    // check that variant's got flagged when they were supposed to.

  }

  /**
   * This method will predict the phenotype given a certain site, given the association results
   *
   * @param sampleObservations An array containing tuples in which the first element is the coded genotype.
   *                           The second is an Array[Double] representing the phenotypes, where the first
   *                           element in the array is the phenotype to regress and the rest are to be treated as
   *                           covariates. The third is the sampleid.
   * @param association  An Association object that specifies the model trained for this locus
   * @return An array of results with the model applied to the observations
   */

  def predictSite(sampleObservations: Array[(Double, Array[Double], String)],
                  association: Association): Array[(String, (Double, Double))] = {
    // transform the data in to design matrix and y matrix compatible with mllib's logistic regresion
    val observationLength = sampleObservations(0)._2.length
    val numObservations = sampleObservations.length
    val lp = new Array[LabeledPoint](numObservations)

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

    val statistics = association.statistics
    val b = statistics("weights").asInstanceOf[Array[Double]]

    // TODO: Check that this actually matches the samples with the right results.
    // receive 0/1 results from datapoints and model
    val results = predict(lp, b)
    samples zip results
  }

  def predict(lpArray: Array[LabeledPoint], b: Array[Double]): Array[(Double, Double)] = {
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
}
