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

import java.io.{ File, FileNotFoundException }

import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import org.bdgenomics.adam.cli.Vcf2ADAM
import java.nio.file.{ Files, Paths }

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset }
import net.fnothaft.gnocchi.models.{ Association, AuxEncoders, Phenotype }

object EvaluateModel extends BDGCommandCompanion {
  val commandName = "evaluateModel"
  val commandDescription = "Fill this out later!!"

  def apply(cmdLine: Array[String]) = {
    new EvaluateModel(Args4j[EvaluateModelArgs](cmdLine))
  }
}

class EvaluateModelArgs extends RegressPhenotypesArgs {
  @Argument(required = true, metaVar = "SNPS", usage = "The IDs of the SNPs to evaluate the model on.", index = 4)
  var snps: String = _
}

class EvaluateModel(protected val evalArgs: EvaluateModelArgs) extends RegressPhenotypes(evalArgs) {
  override val companion = EvaluateModel

  override def run(sc: SparkContext) {

    // Load in genotype data
    val genotypeStates = loadGenotypes(sc)

    // Load in phenotype data
    val phenotypes = loadPhenotypes(sc)

    // Perform analysis
    println("the  problem is in performAnalysis")
    val associations = performEvaluation(genotypeStates, phenotypes, sc)
    println("the problem is in logresults")
    // Log the results
    logResults(associations, sc)
  }

  override def loadGenotypes(sc: SparkContext): Dataset[GenotypeState] = {
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

    // mind filter
    genotypeStates.registerTempTable("genotypeStates")

    val mindDF = sqlContext.sql("SELECT sampleId FROM genotypeStates GROUP BY sampleId HAVING SUM(missingGenotypes)/(COUNT(sampleId)*2) <= %s".format(args.mind))
    val filteredGenotypeStates = genotypeStates.filter($"sampleId".isin(mindDF.collect().map(r => r(0)): _*))
    if (evalArgs.snps != null) {
      // Filter out only specified snps
      // TODO: Clean this
      val snps = evalArgs.snps.split(',')
      filteredGenotypeStates.filter($"contig".isin(snps)).as[GenotypeState]
    }
    filteredGenotypeStates.as[GenotypeState]
  }

  def performEvaluation(genotypeStates: Dataset[GenotypeState],
                        phenotypes: RDD[Phenotype[Array[Double]]],
                        sc: SparkContext): Dataset[Array[(String, Double)]] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val contextOption = Option(sc)
    import AuxEncoders._
    val associations = args.associationType match {
      case "ADDITIVE_LOGISTIC" => AdditiveLogisticEvaluation(genotypeStates.rdd, phenotypes, contextOption)
    }
    sqlContext.createDataset(associations)
  }

  // FIXME: Make this right
  def logResults(results: Dataset[Array[(String, Double)]],
                 sc: SparkContext) = {
    // // save dataset
    // val sqlContext = SQLContext.getOrCreate(sc)
    // val resultsFile = new File(args.associations)
    // if (resultsFile.exists) {
    //   FileUtils.deleteDirectory(resultsFile)
    // }
    // if (args.saveAsText) {
    //   results.rdd.map(r => "%s, %s, %s"
    //     .format(r.variant.getContig.getContigName,
    //       r.variant.getContig.getContigMD5, exp(r.logPValue).toString))
    //     .saveAsTextFile(args.associations)
    // } else {
    //   results.toDF.write.parquet(args.associations)
    // }
    // TODO: Do something!
  }
}

