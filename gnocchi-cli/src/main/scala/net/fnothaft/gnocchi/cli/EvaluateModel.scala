/**
 * Copyright 2015 Frank Austin Nothaft and Taner Dagdelen
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

import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import net.fnothaft.gnocchi.models.{ Association, Phenotype }
import org.apache.spark.sql.functions._
import net.fnothaft.gnocchi.association.Ensembler
import org.apache.hadoop.fs.{ FileSystem, Path }

import scala.collection.mutable.ListBuffer

object EvaluateModel extends BDGCommandCompanion {
  val commandName = "EvaluateModel"
  val commandDescription = "Fill this out later!!"

  def apply(cmdLine: Array[String]) = {
    new EvaluateModel(Args4j[EvaluateModelArgs](cmdLine))
  }
}

class EvaluateModelArgs extends RegressPhenotypesArgs {
  @Argument(required = true, metaVar = "SNPS", usage = "The IDs of the SNPs to evaluate the model on.", index = 4)
  var snps: String = _

  @Argument(required = true, metaVar = "RESULTS", usage = "The location to save results to.", index = 5)
  var results: String = _

  @Argument(required = false, metaVar = "ENSEMBLE_METHOD", usage = "The method used to combine results of SNPs. Options are MAX or AVG.", index = 6)
  var ensembleMethod: String = "AVG"

  @Args4jOption(required = false, name = "ENSEMBLE_WEIGHTS", usage = "The weights to be used in the ensembler's weighted average call.")
  var ensembleWeights: String = ""

  @Args4jOption(required = false, name = "-kfold", usage = "The number of folds to split into using Monte Carlo CV.")
  var kfold = 1

  @Args4jOption(required = false, name = "-numProgressiveSplits", usage = "The number of splits for progressive validation.")
  var numProgressiveSplits = 1

  @Args4jOption(required = false, name = "-monteCarlo", usage = "Use MonteCarlo cross validation instead of kfolds cross validation")
  var monteCarlo = false

  @Args4jOption(required = false, name = "-end", usage = "Uses final fold for testing of all progressive models in progressive validation regression")
  var end = false

  @Args4jOption(required = false, name = "-threshold", usage = "The threshold by which to round the resulting classes. Default is 0.5")
  var threshold = 0.5
}

class EvaluateModel(protected val args: EvaluateModelArgs) extends BDGSparkCommand[EvaluateModelArgs] {
  override val companion = EvaluateModel
  var kcount = 0

  val totalsArray = Array.fill[EvalResult](args.kfold)(new EvalResult())

  override def run(sc: SparkContext) {

    // Load in genotype data filtering out any SNPs not provided in command line
    val genotypeStates = loadGenotypes(sc)

    // instantiate regressPhenotypes obj
    val regPheno = new RegressPhenotypes(args)

    // Load in phenotype data
    val phenotypes = regPheno.loadPhenotypes(sc)

    // perform cross validation
    val crossValResultsArray = performEvaluation(genotypeStates, phenotypes, sc)

    // evaluate results
    println("crossValResultsArray.length: " + crossValResultsArray.length)
    val resultsArray = evaluate(crossValResultsArray)

    // log results
    println("resultsArray.length: " + resultsArray.length)
    logResults(resultsArray)
  }

  def logKFold(): Unit = {
    for (p <- totalsArray.indices) {
      println(s"---------Progressive split $p " + "-" * 30)
      println("Percent of samples with actual 0 phenotype: " + (totalsArray(p).totalPZA.sum / totalsArray(p).totalPZA.length).toString)
      println("Percent of samples with actual 1 phenotype: " + (totalsArray(p).totalPOA.sum / totalsArray(p).totalPOA.length).toString)
      println("Percent of samples predicted to be 0 but actually were 1:" + (totalsArray(p).totalPPZAO.sum / totalsArray(p).totalPPZAO.length).toString)
      println(s"Percent of samples predicted to be 1 but actually were 0: " + (totalsArray(p).totalPPOAZ.sum / totalsArray(p).totalPPOAZ.length).toString)
      println(s"Percent of samples predicted to be 0: " + (totalsArray(p).totalPPZ.sum / totalsArray(p).totalPPZ.length).toString)
      println(s"Percent of samples predicted to be 1: " + (totalsArray(p).totalPPO.sum / totalsArray(p).totalPPO.length).toString)
    }
  }

  def loadGenotypes(sc: SparkContext): Dataset[GenotypeState] = {
    // set up sqlContext
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val absAssociationPath = new Path(args.associations)
    val fs = absAssociationPath.getFileSystem(sc.hadoopConfiguration)
    // val absAssociationStr = fs.getFileStatus(relAssociationPath).getPath.toString
    val parquetInputDestination = absAssociationPath.toString.split("/").reverse.drop(1).reverse.mkString("/") + "/parquetInputFiles/"
    val parquetFiles = new Path(parquetInputDestination)

    val vcfPath = args.genotypes

    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
    if (!fs.exists(parquetFiles)) {
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    } else if (args.overwrite) {
      fs.delete(parquetFiles, true)
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    }

    val genotypes = sqlContext.read.format("parquet").load(parquetInputDestination)
    // transform the parquet-formatted genotypes into a dataFrame of GenotypeStates and convert to Dataset.
    val genotypeStates = sqlContext
      .toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = false)
    val genoStatesWithNames = genotypeStates.select(concat($"contig", lit("_"), $"end", lit("_"), $"alt") as "contig",
      genotypeStates("start"),
      genotypeStates("end"),
      genotypeStates("ref"),
      genotypeStates("alt"),
      genotypeStates("sampleId"),
      genotypeStates("genotypeState"),
      genotypeStates("missingGenotypes"))
    println(genoStatesWithNames.take(10).toList)

    // mind filter
    genoStatesWithNames.registerTempTable("genotypeStates")

    val mindDF = sqlContext.sql("SELECT sampleId FROM genotypeStates GROUP BY sampleId HAVING SUM(missingGenotypes)/(COUNT(sampleId)*2) <= %s".format(args.mind))
    var filteredGenotypeStates = genoStatesWithNames.filter($"sampleId".isin(mindDF.collect().map(r => r(0)): _*))
    println("Pre-filtered GenotypeStates: " + filteredGenotypeStates.take(5).toList)
    if (args.snps != null) {
      // Filter out only specified snps
      // TODO: Clean this
      val snps = args.snps.split(',')
      filteredGenotypeStates = filteredGenotypeStates.filter(filteredGenotypeStates("contig").isin(snps: _*))
    }
    println("Post-filtered GenotypeStates: " + filteredGenotypeStates.take(5).toList)
    filteredGenotypeStates.as[GenotypeState]
  }

  def performEvaluation(genotypeStates: Dataset[GenotypeState],
                        phenotypes: RDD[Phenotype[Array[Double]]],
                        sc: SparkContext): Array[RDD[(Array[(String, (Double, Double))], Association)]] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val contextOption = Option(sc)
    val k = args.kfold
    val n = args.numProgressiveSplits
    val monte = args.monteCarlo
    val evaluations = args.associationType match {
      case "ADDITIVE_LOGISTIC" => {

        if (n == 1) {
          if (k == 1) {
            AdditiveLogisticEvaluation(genotypeStates.rdd, phenotypes, scOption = contextOption, k = args.kfold, n = args.numProgressiveSplits, sc, monte = args.monteCarlo, threshold = args.threshold)
          } else {
            if (monte) {
              AdditiveLogisticMonteCarloEvaluation(genotypeStates.rdd, phenotypes, scOption = contextOption, k = args.kfold, n = args.numProgressiveSplits, sc, monte = args.monteCarlo, threshold = args.threshold)
            } else {
              AdditiveLogisticKfoldsEvaluation(genotypeStates.rdd, phenotypes, scOption = contextOption, k = args.kfold, n = args.numProgressiveSplits, sc, monte = args.monteCarlo, threshold = args.threshold)
            }
          }
        } else {
          if (k == 1) {
            if (args.end) {
              AdditiveLogisticEndProgressiveEvaluation(genotypeStates.rdd, phenotypes, scOption = contextOption, k = args.kfold, n = args.numProgressiveSplits, sc, monte = args.monteCarlo, threshold = args.threshold)
            } else {
              AdditiveLogisticProgressiveEvaluation(genotypeStates.rdd, phenotypes, scOption = contextOption, k = args.kfold, n = args.numProgressiveSplits, sc, monte = args.monteCarlo, threshold = args.threshold)
            }
          } else {
            assert(false, "cross validation not possible for progressive validation.")
          }
        }
      }
    }
    evaluations.asInstanceOf[Array[RDD[(Array[(String, (Double, Double))], Association)]]]
  }

  def evaluate(evalArray: Array[RDD[(Array[(String, (Double, Double))], Association)]]): Array[EvalResult] = {
    val resultsArray = new Array[EvalResult](evalArray.length)
    for (i <- evalArray.indices) {
      resultsArray(i) = evaluateResult(evalArray(i))
    }
    resultsArray
  }

  def evaluateResult(toEvaluate: RDD[(Array[(String, (Double, Double))], Association)]): EvalResult = {
    val evalResult = new EvalResult
    val numTrainingSamples = toEvaluate.take(1)(0)._2.statistics("numSamples").asInstanceOf[Int]

    val ensembleMethod = args.ensembleMethod
    var ensembleWeights = Array[Double]()
    if (args.ensembleWeights != "") {
      ensembleWeights = args.ensembleWeights.split(",").map(x => x.toDouble)
    }

    val resultsBySample = toEvaluate.flatMap(ipaa => {
      var sampleId = ipaa._1(0)._1
      var prediction = ipaa._1(0)._2._1
      var actual = ipaa._1(0)._2._2
      val association = ipaa._2
      var toRet = Array((sampleId, (prediction, actual, association)))
      if (ipaa._1.length > 1) {
        for (i <- 1 until ipaa._1.length) {
          var sampleId = ipaa._1(i)._1
          var prediction = ipaa._1(i)._2._1
          var actual = ipaa._1(i)._2._2
          toRet = toRet :+ (sampleId, (prediction, actual, association))
        }
      }
      toRet.toList
    }).groupByKey

      // ensemble the SNP models for each sample
      .map(sample => {
        val (sampleId, snpArray) = sample
        (sampleId, Ensembler(ensembleMethod, snpArray.toArray, ensembleWeights))
      })

    // compute final results
    val resArray = resultsBySample.collect
    val numSamples = resArray.length
    var numZeroActual = 0.0
    var numZeroPred = 0.0
    var numZeroPredOneActual = 0.0
    var numOnePredZeroActual = 0.0
    for (i <- resArray.indices) {
      val pred = resArray(i)._2._1
      val actual = resArray(i)._2._2
      if (actual == 0.0) {
        numZeroActual += 1.0
        if (pred == 1.0) {
          numOnePredZeroActual += 1.0
        } else {
          numZeroPred += 1.0
        }
      } else {
        if (pred == 0.0) {
          numZeroPredOneActual += 1.0
        }
      }
    }
    val percentZeroActual = numZeroActual / numSamples
    val percentOneActual = 1 - percentZeroActual
    val percentPredZeroActualOne = numZeroPredOneActual / (numSamples - numZeroActual)
    val percentPredOneActualZero = numOnePredZeroActual / numZeroActual
    val percentPredZero = numZeroPred / numSamples
    val percentPredOne = 1 - percentPredZero

    evalResult.totalPZA += percentZeroActual
    evalResult.totalPOA += percentOneActual
    evalResult.totalPPZAO += percentPredZeroActualOne
    evalResult.totalPPOAZ += percentPredOneActualZero
    evalResult.totalPPZ += percentPredZero
    evalResult.totalPPO += percentPredOne
    evalResult.numSamples = numTrainingSamples
    evalResult
  }

  //  /**
  //   * Logs results of an evaluation.
  //   *
  //   * @param results RDD of (Array[(id, (predicted, actual))], Association).
  //   *                The association model contains the weights.
  //   * @param sc the spark context to be used.
  //   */
  //  def logResults(results: RDD[(Array[(String, (Double, Double))], Association)],
  //                 sc: SparkContext,
  //                 evalResult: EvalResult) = {
  def logResults(results: Array[EvalResult]) = {

    // save results
    //    val output = args.results
    //    val sqlContext = SQLContext.getOrCreate(sc)
    //    val outputFile = new File(output)
    //    if (outputFile.exists) {
    //      FileUtils.deleteDirectory(outputFile)
    //    }

    //    val assocs = results.map(site => site._2)
    //    if (args.saveAsText) {
    //      assocs.map(r => "%s, %s, %s"
    //        .format(r.variant.getContig.getContigName,
    //          r.variant.getContig.getContigMD5, exp(r.logPValue).toString))
    //        .saveAsTextFile(outputFile)
    //      valResults.map(r => "%s, %f"
    //        .format(r._1, r._2))
    //        .saveAsTextFile(evalArgs.results)
    //      println(s"Percent of samples with actual unaffected phenotype: $percentZeroActual")
    //      println(s"Percent of samples with actual affected phenotype: $percentOneActual")
    //      println(s"Percent of samples predicted to be unaffected but actually were affected: $percentPredZeroActualOne")
    //      println(s"Percent of samples predicted to be affected but actually were unaffected: $percentPredOneActualZero")
    //      println(s"Percent of samples predicted to be unaffected: $percentPredZero")
    //      println(s"Percent of samples predicted to be affected: $percentPredOne")
    //    } else {
    //      sqlContext.createDataFrame(assocs).write.parquet(outputFile)
    //      sqlContext.createDataFrame(resultsBySample).write.parquet(args.results)
    //      println(s"Percent of samples with actual unaffected phenotype: $percentZeroActual")
    //      println(s"Percent of samples with actual affected phenotype: $percentOneActual")
    //      println(s"Percent of samples predicted to be unaffected but actually were affected: $percentPredZeroActualOne")
    //      println(s"Percent of samples predicted to be affected but actually were unaffected: $percentPredOneActualZero")
    //      println(s"Percent of samples predicted to be unaffected: $percentPredZero")
    //      println(s"Percent of samples predicted to be affected: $percentPredOne")
    //    }
    //  }
    val sumEval = new EvalResult
    var splitType = "Fold"
    if (args.numProgressiveSplits > 1) {
      splitType = "NumSamples"
    }
    for (i <- results.indices) {
      val result = results(i)
      val samples = result.numSamples
      if (splitType == "Fold") {
        println(s"\n----------------------Fold $i---------------------------------------------------------\n")
      } else {
        println(s"\n----------------------Number of Samples $samples---------------------------------------------------------\n")
      }
      val pza = result.totalPZA.head
      println(s"Percent of samples with actual unaffected phenotype: $pza")
      val poa = result.totalPOA.head
      println(s"Percent of samples with actual affected phenotype: $poa")
      val ppzao = result.totalPPZAO.head
      println(s"Percent of samples predicted to be unaffected but actually were affected: $ppzao")
      val ppoaz = result.totalPPOAZ.head
      println(s"Percent of samples predicted to be affected but actually were unaffected: $ppoaz")
      val ppz = result.totalPPZ.head
      println(s"Percent of samples predicted to be unaffected: $ppz")
      val ppo = result.totalPPO.head
      println(s"Percent of samples predicted to be affected: $ppo")
      sumEval.totalPZA += result.totalPZA.head
      sumEval.totalPOA += result.totalPOA.head
      sumEval.totalPPZAO += result.totalPPZAO.head
      sumEval.totalPPOAZ += result.totalPPOAZ.head
      sumEval.totalPPZ += result.totalPPZ.head
      sumEval.totalPPO += result.totalPPO.head
    }
    if (splitType == "Fold") {
      val avgPZA = sumEval.totalPZA.sum / results.length
      val avgPOA = sumEval.totalPOA.sum / results.length
      val avgPPZAO = sumEval.totalPPZAO.sum / results.length
      val avgPPOAZ = sumEval.totalPPOAZ.sum / results.length
      val avgPPZ = sumEval.totalPPZ.sum / results.length
      val avgPPO = sumEval.totalPPO.sum / results.length

      println("\n------------------------------- Average ---------------------------------------------\n")
      println(s"Average percent of samples with actual unaffected phenotype: $avgPZA")
      println(s"Average percent of samples with actual affected phenotype: $avgPOA")
      println(s"Average percent of samples predicted to be unaffected but actually were affected: $avgPPZAO")
      println(s"Average percent of samples predicted to be affected but actually were unaffected: $avgPPOAZ")
      println(s"Average percent of samples predicted to be unaffected: $avgPPZ")
      println(s"Avergae percent of samples predicted to be affected: $avgPPO")
    }

  }
}

class EvalResult {
  var numSamples = 0
  val totalPZA = new ListBuffer[Double]
  val totalPOA = new ListBuffer[Double]
  val totalPPZAO = new ListBuffer[Double]
  val totalPPOAZ = new ListBuffer[Double]
  val totalPPZ = new ListBuffer[Double]
  val totalPPO = new ListBuffer[Double]
}

