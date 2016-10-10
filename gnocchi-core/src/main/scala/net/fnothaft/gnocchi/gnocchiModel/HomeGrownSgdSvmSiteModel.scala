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

import net.fnothaft.gnocchi.models.{SiteModel, _}
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{Contig, Variant}
import breeze.linalg._

import scala.util.Random


trait HomeGrownSgdSvmSiteModel extends SiteModel {

  val latestTR = new TestResult()
  var numSamples = 0.0
  val variables: Array[String]
  val description: String
  var coeffs: DenseVector[Double]
  var lambda: Double
  val parameterDescriptions = Array[String](".coeffs = coefficients", ".lambda = regularization term", ".numSamples = number of samples seen", ".bias = y-intercept")

  protected def update(xg: GenotypeState, yp: Phenotype[Array[Double]]): Unit = {
    val (x,y) = transform(xg,yp)
    updateCoeffs(x,y)
  }

  protected def batchUpdate(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): Unit = {
    val samplesAndLabels = batchTransform(x,y)
    for (single<-samplesAndLabels) {
      val x = single._1
      val y = single._2
      updateCoeffs(x,y)
    }
  }

  def transform(xg: GenotypeState, yp: Phenotype[Array[Double]]): (DenseVector[Double], Double) = {
    // transform the data into design vector (x) and label (y)
    val xList = List[Double](xg.genotypeState.toDouble) ::: yp.value.drop(1).toList
    val x = DenseVector(xList.toArray)
    val phenos = yp.value
    val y = phenos(0)
    (x,y)
  }

  def batchTransform(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): Array[(DenseVector[Double], Double)] = {
    val xg = x.map(genoState => (genoState.sampleId, genoState))
    val yp = y.map(pheno => (pheno.sampleId, pheno))
    val designVectors = xg.cogroup(yp)
                          .map(kvv => {
                            //unpack the id-genopheno pairs
                            val (id, genopheno) = kvv
                            val geno = genopheno._1
                            val pheno = genopheno._2
                            transform(geno.toList.head,pheno.toList.head
                          })
    designVectors.collect()
  }

  protected def updateCoeffs(x: DenseVector[Double], y: Double) {
    if (coeffs.length == 0) {
      //initialize the parameters vector using the sample x
      coeffs = DenseVector.zeros[Double](x.length)
    }
    if ((coeffs dot x) >= 1) {
      val updatedCoeffs = (numSamples / (numSamples + 1)) * coeffs + (1 / (lambda * (numSamples + 1))) * x
      coeffs = updatedCoeffs
    } else {
      val updatedCoeffs = (numSamples / (numSamples + 1)) * coeffs
      coeffs = updatedCoeffs
    }
    numSamples = numSamples + 1
  }

  // TODO: use a QR factorization to do a recompute over the whole dataset
  //protected def recompute(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): Unit = {
    //
  //}



  protected def predict(desc: String, xg: GenotypeState, yp: Phenotype[Array[Double]]): (Double, Prediction) = {
    val (x,y) = transform(xg, yp)
    val pred = Prediction(sampleId=xg.sampleId,
                              predictedY = x.dot(coeffs),
                              description = desc,
                              genotype =xg,
                              phenotype = yp)
    return (y, pred)
  }

  protected def batchPredict(desc: String, x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): RDD[(Double, Prediction)] = {
    val xg = x.map(genoState => (genoState.sampleId, genoState))
    val yp = y.map(pheno => (pheno.sampleId, pheno))
    val predictions = xg.cogroup(yp)
      .map(kvv => {
        //unpack the id-genopheno pairs
        val (id, genopheno) = kvv
        val geno = genopheno._1
        val pheno = genopheno._2
        predict(desc, geno.toList(0), pheno.toList(0))
      })
    return predictions
  }

  protected def evaluate(x: RDD[GenotypeState], y: RDD[Phenotype[Array[Double]]]): TestResult = {
    // TODO: get and store variant info

    // computes the mean squared error of the predictions
    val predictions = batchPredict("no_desc", x, y)
    val errors = predictions.map(kvv => {
      val (label, pred) = kvv
      Math.pow(label - pred.predictedY,2)
    })
    val rows = errors.count().toInt
    val squaredError =errors.sum()
    val result = TestResult(score = squaredError/rows,
                            numSamples = rows,
                            lossFn = "Squared Error",
                            variant = new Variant(),
                            desc = "") //TODO: format descriptions for output.
    result
  }

  def clearModel {
    numSamples = 0.0
    var coeffs = DenseVector[Double]()
  }

  def evaluateOnTransformedData(samplesAndLabels: Array[(DenseVector[Double], Double)]): Double = {
    var sumOfErrors = 0.0
    for (sample <- samplesAndLabels) {
      val x = sample._1
      val y = sample._2
      val pred = x.dot(coeffs)
      val error = Math.pow(y - pred, 2)
      sumOfErrors = sumOfErrors + error
    }
    sumOfErrors/samplesAndLabels.length
  }

  protected def crossValidate(xg: RDD[GenotypeState], yp: RDD[Phenotype[Array[Double]]]): TestResult = {
    val numRows = xg.count().toInt
    val numTraining = (numRows*0.8).toInt
    // transform the data
    val samplesAndLabels = batchTransform(xg,yp)
    var mserrors = 0.0
    // perform train/test cycle 10 times
    for (i <- 0 to 10) {
      // shuffle data
      val shuffledData = Random.shuffle(samplesAndLabels.toList)
      // randomly partition the data into training/test training on 80% and testing on 20%.
      val trainingData = shuffledData.slice(0, numTraining).toArray
      val testData = shuffledData.slice(numTraining,numRows).toArray
      // train model on the 80%
      for (sample<-trainingData) {
        val x = sample._1
        val y = sample._2
        updateCoeffs(x,y)
      }
      // test on the 20%
      val mse = evaluateOnTransformedData(trainingData)
      mserrors = mserrors + mse
    }
    // average the mean squared error
    val avg = mserrors/10

    // make TestResult
    TestResult(score = avg,
      numSamples = numRows,
      lossFn="Squared Error",
      variant = new Variant(),
      desc = "Average mean squared error from 10-fold cross-validation (80/20 training/test)")
  }

  protected def saveSite()

