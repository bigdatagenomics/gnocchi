/**
 * Copyright 2016 Frank Austin Nothaft, Taner Dagdelen
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
package net.fnothaft.gnocchi.association

import breeze.linalg.DenseVector
import net.fnothaft.gnocchi.GnocchiFunSuite
import org.bdgenomics.adam.models.ReferenceRegion
import net.fnothaft.gnocchi.models.Association
import org.bdgenomics.formats.avro.Variant

class LogisticValidationRegressionSuite extends GnocchiFunSuite {

  sparkTest("Test logistic regression prediction on binary data") {
    // read in the data from binary.csv
    // data comes from: http://www.ats.ucla.edu/stat/sas/dae/binary.sas7bdat
    // results can be found here: http://www.ats.ucla.edu/stat/sas/dae/logit.htm
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binary.csv").getFile
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows

    // transform it into the right format
    val sampleObservations = data.map(row => {
      val geno: Double = row(0)
      val covars: Array[Double] = row.slice(1, 3)
      val phenos: Array[Double] = Array(row(3)) ++ covars
      (geno, phenos, "")
    }).collect()

    // generate array of expected results for each sample, based on given phenotype
    val expectedResults = data.map(row => {
      val predicted: Double = row(4)
      val actual: Double = row(3)
      ("", (actual, predicted))
    }).collect()

    val fakeVariant = new Variant()
    val fakePhenotype = "acceptance"
    // Our known fitting model
    val weights = Array(-3.4495484, .0022939, .77701357, -0.5600314)
    val statistics = Map("weights" -> weights)
    val assoc = Association(fakeVariant, "", 0.0, statistics)

    // Array[(String, Double)] where String is sampleid and Double is predicted value
    val predictionResult = AdditiveLogisticEvaluation.predictSite(sampleObservations, assoc)

    // Assert that the predictions result in the same as the actual phenotype (on dummy set)
    for (i <- predictionResult.indices) {
      if (predictionResult(i) != expectedResults(i)) {
        print("Error --> ")
      }
      println("Ours: " + predictionResult(i) + " | Theirs: " + expectedResults(i))
    }
    println(predictionResult.indices.map(i => {
      predictionResult(i) == expectedResults(i)
    }).count(b => { b }))
    assert(predictionResult.sameElements(expectedResults))
  }

  sparkTest("Test logistic regression prediction on binary data with a different threshold") {
    // Reads in the binaryClean data, which is the same as the above data but removes any
    // duplicate samples. The Fourth row (0 indexed) is the prediction results generated
    // from a logisitic regression model in excel with a threshold classification value of 0.3
    val pathToFile = ClassLoader.getSystemClassLoader.getResource("binaryThreshold3.csv").getFile
    val csv = sc.textFile(pathToFile)
    val data = csv.map(line => line.split(",").map(elem => elem.toDouble)) //get rows

    // transform it into the right format
    val sampleObservations = data.map(row => {
      val geno: Double = row(0)
      val covars: Array[Double] = row.slice(1, 3)
      val phenos: Array[Double] = Array(row(3)) ++ covars
      (geno, phenos, "")
    }).collect()

    // generate array of expected results for each sample, based on given phenotype
    val expectedResults = data.map(row => {
      val actual: Double = row(3)
      val predicted: Double = row(4)
      ("", (actual, predicted))
    }).collect()

    val fakeVariant = new Variant()
    val fakePhenotype = "acceptance"
    // Our known fitting model

    val weights = Array(-3.351609998, 0.002338182, 0.742468226, -0.568764356)
    val statistics = Map("weights" -> weights)
    val assoc = Association(fakeVariant, "", 0.0, statistics)

    // Array[(String, Double)] where String is sampleid and Double is predicted value
    val predictionResult = AdditiveLogisticEvaluation.predictSite(sampleObservations, assoc, 0.3)

    // Assert that the predictions result in the same as the actual phenotype (on dummy set)
    for (i <- predictionResult.indices) {
      if (predictionResult(i) != expectedResults(i)) {
        print("Error --> ")
      }
      println("Ours: " + predictionResult(i) + " | Theirs: " + expectedResults(i))
    }
    println(predictionResult.indices.map(i => {
      predictionResult(i) == expectedResults(i)
    }).count(b => { b }))
    assert(predictionResult.sameElements(expectedResults))
  }
}

