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

import net.fnothaft.gnocchi.models.{GenotypeState, Phenotype}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint


trait SGDSiteSVM extends SGDSiteRegression {

  def buildOrUpdateSiteModel(sc: SparkContext, siteData: RDD[(GenotypeState, Phenotype[Array[Double]])], pathOption: Option[String]): SVMModel = {

    // transform the data
    val siteLabeledPoints = transformData(siteData)

    // load or generate the model
    val (svmmodel, weights) = loadOrGenerateModel(sc, pathOption)

    // fit/update and return the model
    updateModel(siteLabeledPoints, weights, svmmodel)
  }

  def transformData(siteData: RDD[(GenotypeState, Phenotype[Array[Double]])]): RDD[LabeledPoint] = {
    // transform the data into design vector (x) and label (y)
    siteData.map(sample => {
      val (genoState, phenotype) = sample
      val label = phenotype.value(0)
      clipOrKeepState(genoState)
      val featureList = List[Double](genoState.genotypeState) ::: phenotype.value.drop(1).toList
      val features = new DenseVector(featureList.toArray)
      new LabeledPoint(label, features)
    })
  }

  def loadOrGenerateModel(sc: SparkContext, pathToModel: Option[String]): (SVMWithSGD, DenseVector) = {
    val stepSize = 1 / (numSamples + 1)
    val numIterations = 1
    val regParam = 0.01
    val miniBatchFraction = 1.0
    val sgdmodel = new SVMWithSGD()
    sgdmodel.optimizer
      .setStepSize(stepSize)
      .setNumIterations(numIterations)
      .setRegParam(regParam)
      .setMiniBatchFraction(miniBatchFraction)
    var weights: DenseVector = DenseVector(Array.fill(numFeatures)(0))
    if (hasModel(pathToModel)) {
      val model = SVMModel.load(sc, pathToModel.get)
      weights = model.weights.toDense
    }
    (sgdmodel, weights)
  }

  def hasModel(path: Option[String]) = path match {
    case Some(s) => true
    case None => false
  }

  def updateModel(siteLabeledPoints: RDD[LabeledPoint], weights: DenseVector, svmWithSGD: SVMWithSGD): SVMModel = {
    // fit the model, setting the starting point of the weights as zeros if new model or the old weights if a model was loaded
    svmWithSGD.run(siteLabeledPoints, weights)
  }
}

  object AdditiveSVMWithSGD extends SGDSiteSVM with SGDAdditive {
    val regressionName = "Additive SVM with SGD"
  }

  object DominantSVMWithSGD extends SGDSiteSVM with SGDDominant {
    val regressionName = "Dominant SVM with SGD"
  }




