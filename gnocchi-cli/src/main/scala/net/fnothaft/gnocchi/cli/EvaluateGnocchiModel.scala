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
package net.fnothaft.gnocchi.cli

import java.io.File

import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.gnocchiModel._
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
//import net.fnothaft.gnocchi.association.Ensembler TODO: pull in the ensembler code or predict won't work.
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticGnocchiModel

import scala.collection.mutable.ListBuffer

object EvaluateGnocchiModel extends BDGCommandCompanion {
  val commandName = "EvaluateGnocchiModel"
  val commandDescription = "Fill this out later!!"

  def apply(cmdLine: Array[String]) = {
    new EvaluateGnocchiModel(Args4j[EvaluateGnocchiModelArgs](cmdLine))
  }
}

class EvaluateGnocchiModelArgs extends RegressPhenotypesArgs {

  @Args4jOption(required = true, name = "-modelLocation", usage = "The location of the model to load.")
  var modelLocation: String = _

  @Args4jOption(required = true, name = "-savePredictionsTo", usage = "The location to save predictions to.")
  var saveTo: String = _

  @Args4jOption(required = true, name = "-saveEvalResultsTo", usage = "The location to save evaluation results to.")
  var evalTo: String = _

}

class EvaluateGnocchiModel(protected val args: EvaluateGnocchiModelArgs) extends BDGSparkCommand[EvaluateGnocchiModelArgs] {
  override val companion = PredictWithGnocchiModel

  override def run(sc: SparkContext) {

    // instantiate regressPhenotypes obj
    val regPheno = new RegressPhenotypes(args)

    // Load in genotype data filtering out any SNPs not provided in command line
    val genotypeStates = regPheno.loadGenotypes(sc)

    // Load in phenotype data
    val phenotypes = regPheno.loadPhenotypes(sc)

    // load model TODO: Write load GnocchiModel object
    val model = LoadGnocchiModel(args.modelLocation)

    // make predictions on new data
    val predictions = model.predict(genotypeStates.rdd, phenotypes, sc)

    // save the predictions TODO: write savePredictions function
    //    savePredictions(predictions)

    // evaluate the model based on predictions
    //    evaluateModel(predictions)
    // save the model
    //    gnocchiModel.save(args.saveTo)
  }

  // TODO: adapt model.predict so that the reults can be fed into evaluateResult.
  //  def evaluateResult(toEvaluate: RDD[(Array[(String, (Double, Double))], Association)]): EvalResult = {
  //    val evalResult = new EvalResult
  //    val numTrainingSamples = toEvaluate.take(1)(0)._2.statistics("numSamples").asInstanceOf[Int]
  //
  //    val ensembleMethod = args.ensembleMethod
  //    var ensembleWeights = Array[Double]()
  //    if (args.ensembleWeights != "") {
  //      ensembleWeights = args.ensembleWeights.split(",").map(x => x.toDouble)
  //    }
  //
  //    val resultsBySample = toEvaluate.flatMap(ipaa => {
  //      var sampleId = ipaa._1(0)._1
  //      var prediction = ipaa._1(0)._2._1
  //      var actual = ipaa._1(0)._2._2
  //      val association = ipaa._2
  //      var toRet = Array((sampleId, (prediction, actual, association)))
  //      if (ipaa._1.length > 1) {
  //        for (i <- 1 until ipaa._1.length) {
  //          var sampleId = ipaa._1(i)._1
  //          var prediction = ipaa._1(i)._2._1
  //          var actual = ipaa._1(i)._2._2
  //          toRet = toRet :+ (sampleId, (prediction, actual, association))
  //        }
  //      }
  //      toRet.toList
  //    }).groupByKey
  //
  //      // ensemble the SNP models for each sample
  //      .map(sample => {
  //      val (sampleId, snpArray) = sample
  //      (sampleId, Ensembler(ensembleMethod, snpArray.toArray, ensembleWeights))
  //    })
  //
  //    // compute final results
  //    val resArray = resultsBySample.collect
  //    val numSamples = resArray.length
  //    var numZeroActual = 0.0
  //    var numZeroPred = 0.0
  //    var numZeroPredOneActual = 0.0
  //    var numOnePredZeroActual = 0.0
  //    for (i <- resArray.indices) {
  //      val pred = resArray(i)._2._1
  //      val actual = resArray(i)._2._2
  //      if (actual == 0.0) {
  //        numZeroActual += 1.0
  //        if (pred == 1.0) {
  //          numOnePredZeroActual += 1.0
  //        } else {
  //          numZeroPred += 1.0
  //        }
  //      } else {
  //        if (pred == 0.0) {
  //          numZeroPredOneActual += 1.0
  //        }
  //      }
  //    }
  //    val percentZeroActual = numZeroActual / numSamples
  //    val percentOneActual = 1 - percentZeroActual
  //    val percentPredZeroActualOne = numZeroPredOneActual / (numSamples - numZeroActual)
  //    val percentPredOneActualZero = numOnePredZeroActual / numZeroActual
  //    val percentPredZero = numZeroPred / numSamples
  //    val percentPredOne = 1 - percentPredZero
  //
  //    evalResult.totalPZA += percentZeroActual
  //    evalResult.totalPOA += percentOneActual
  //    evalResult.totalPPZAO += percentPredZeroActualOne
  //    evalResult.totalPPOAZ += percentPredOneActualZero
  //    evalResult.totalPPZ += percentPredZero
  //    evalResult.totalPPO += percentPredOne
  //    evalResult.numSamples = numTrainingSamples
  //    evalResult
  //  }
}
