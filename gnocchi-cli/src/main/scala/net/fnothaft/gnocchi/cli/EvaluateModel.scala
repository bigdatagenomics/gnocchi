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

import java.io.File

import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.adam.cli.Vcf2ADAM
import breeze.numerics.exp
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import net.fnothaft.gnocchi.models.{ Association, Phenotype }
import org.apache.spark.sql.functions._
import net.fnothaft.gnocchi.association.Ensembler

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

  @Args4jOption(required = false, name = "KFOLD", usage = "The number of folds to split into using Monte Carlo CV.")
  var kfold = 10

}

class EvaluateModel(protected val args: EvaluateModelArgs) extends BDGSparkCommand[EvaluateModelArgs] {
  override val companion = EvaluateModel
  var kcount = 0
  val totalPZA = new ListBuffer[Double]
  val totalPOA = new ListBuffer[Double]
  val totalPPZAO = new ListBuffer[Double]
  val totalPPOAZ = new ListBuffer[Double]
  val totalPPZ = new ListBuffer[Double]
  val totalPPO = new ListBuffer[Double]
  override def run(sc: SparkContext) {

    // Load in genotype data
    val genotypeStates = loadGenotypes(sc)

    // instantiate regressPhenotypes obj
    val regPheno = new RegressPhenotypes(args)

    // Load in phenotype data
    val phenotypes = regPheno.loadPhenotypes(sc)
    while (kcount < args.kfold) {
      // Perform analysis
      val results = performEvaluation(genotypeStates, phenotypes, sc)
      // Log the results
      logResults(results, sc)
      kcount += 1
    }
    logKFold()
  }

  def logKFold(): Unit = {
    println("-" * 30)
    println("Percent of samples with actual 0 phenotype: " + (totalPZA.sum / totalPZA.length).toString)
    println("Percent of samples with actual 1 phenotype: " + (totalPOA.sum / totalPOA.length).toString)
    println("Percent of samples predicted to be 0 but actually were 1:" + (totalPPZAO.sum / totalPPZAO.length).toString)
    println(s"Percent of samples predicted to be 1 but actually were 0: " + (totalPPOAZ.sum / totalPPOAZ.length).toString)
    println(s"Percent of samples predicted to be 0: " + (totalPPZ.sum / totalPPZ.length).toString)
    println(s"Percent of samples predicted to be 1: " + (totalPPO.sum / totalPPO.length).toString)
  }

  def loadGenotypes(sc: SparkContext): Dataset[GenotypeState] = {
    // set up sqlContext
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val absAssociationPath = new File(args.associations).getAbsolutePath
    var parquetInputDestination = absAssociationPath.split("/").reverse.drop(1).reverse.mkString("/")
    parquetInputDestination = parquetInputDestination + "/parquetInputFiles/"
    val parquetFiles = new File(parquetInputDestination)

    val vcfPath = args.genotypes
    val posAndIds = GetVariantIds(sc, vcfPath)

    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
    if (!parquetFiles.getAbsoluteFile.exists) {
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    } else if (args.overwrite) {
      FileUtils.deleteDirectory(parquetFiles)
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
                        sc: SparkContext): RDD[(Array[(String, (Double, Double))], Association)] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val contextOption = Option(sc)
    val evaluations = args.associationType match {
      case "ADDITIVE_LOGISTIC" => AdditiveLogisticEvaluation(genotypeStates.rdd, phenotypes, contextOption, k = args.kfold)
    }
    evaluations
  }

  /**
   * Logs results of an evaluation.
   * FIXME: Make this right.
   *
   * @param results RDD of (Array[(id, (predicted, actual))], Association).
   *                The association model contains the weights.
   * @param sc the spark context to be used.
   */
  def logResults(results: RDD[(Array[(String, (Double, Double))], Association)],
                 sc: SparkContext) = {
    // save dataset
    val outputFile = args.associations + "-" + kcount.toString
    val sqlContext = SQLContext.getOrCreate(sc)
    val assocsFile = new File(outputFile)
    if (assocsFile.exists) {
      FileUtils.deleteDirectory(assocsFile)
    }

    val ensembleMethod = args.ensembleMethod
    var ensembleWeights = Array[Double]()
    if (args.ensembleWeights != "") {
      ensembleWeights = args.ensembleWeights.split(",").map(x => x.toDouble)
    }

    val resultsBySample = results.flatMap(ipaa => {
      var toRet = Array((ipaa._1(0)._1, (ipaa._1(0)._2._1, ipaa._1(0)._2._2, ipaa._2)))
      for (i <- 1 until ipaa._1.length) {
        toRet = toRet :+ (ipaa._1(i)._1, (ipaa._1(i)._2._1, ipaa._1(i)._2._2, ipaa._2))
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

    totalPZA += percentZeroActual
    totalPOA += percentOneActual
    totalPPZAO += percentPredZeroActualOne
    totalPPOAZ += percentPredOneActualZero
    totalPPZ += percentPredZero
    totalPPO += percentPredOne

    //      .map(ipaa => {
    //        val ((sampleId, (pred, actual)), assoc): ((String, (Double, Double)), Association) = ipaa
    //        (sampleId, (assoc, pred, actual))
    //      }).groupByKey
    //
    //
    //      .map(_._1).flatMap(_.toTraversable).map(sampleSite => {
    //      val sampleId = sampleSite._1
    //      val res = sampleSite._2
    //      (sampleId, (if (res._1 == res._2) 1 else 0, 1))
    //    }).reduceByKey((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2)).map(s => {
    //      val sampleId = s._1
    //      val nums = s._2
    //      (sampleId, nums._1.toDouble / nums._2.toDouble)
    //    })
    val assocs = results.map(site => site._2)
    if (args.saveAsText) {
      assocs.map(r => "%s, %s, %s"
        .format(r.variant.getContig.getContigName,
          r.variant.getContig.getContigMD5, exp(r.logPValue).toString))
        .saveAsTextFile(outputFile)
      //      valResults.map(r => "%s, %f"
      //        .format(r._1, r._2))
      //        .saveAsTextFile(evalArgs.results)
      println(s"Percent of samples with actual unaffected phenotype: $percentZeroActual")
      println(s"Percent of samples with actual affected phenotype: $percentOneActual")
      println(s"Percent of samples predicted to be unaffected but actually were affected: $percentPredZeroActualOne")
      println(s"Percent of samples predicted to be affected but actually were unaffected: $percentPredOneActualZero")
      println(s"Percent of samples predicted to be unaffected: $percentPredZero")
      println(s"Percent of samples predicted to be affected: $percentPredOne")
    } else {
      sqlContext.createDataFrame(assocs).write.parquet(outputFile)
      sqlContext.createDataFrame(resultsBySample).write.parquet(args.results)
      println(s"Percent of samples with actual unaffected phenotype: $percentZeroActual")
      println(s"Percent of samples with actual affected phenotype: $percentOneActual")
      println(s"Percent of samples predicted to be unaffected but actually were affected: $percentPredZeroActualOne")
      println(s"Percent of samples predicted to be affected but actually were unaffected: $percentPredOneActualZero")
      println(s"Percent of samples predicted to be unaffected: $percentPredZero")
      println(s"Percent of samples predicted to be affected: $percentPredOne")
    }
  }
}

