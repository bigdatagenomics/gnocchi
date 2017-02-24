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

import java.io.{ File, OutputStreamWriter, PrintWriter }
import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{ MetricsListener, RecordedMetrics }
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import scala.math.exp

import org.bdgenomics.adam.cli.Vcf2ADAM
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ concat, lit }
import net.fnothaft.gnocchi.models.{ Phenotype, Association, AuxEncoders }

object RegressPhenotypes extends BDGCommandCompanion {
  val commandName = "regressPhenotypes"
  val commandDescription = "Pilot code for computing genotype/phenotype associations using ADAM"

  def apply(cmdLine: Array[String]) = {
    new RegressPhenotypes(Args4j[RegressPhenotypesArgs](cmdLine))
  }
}

class RegressPhenotypesArgs extends Args4jBase {
  @Argument(required = true, metaVar = "GENOTYPES", usage = "The genotypes to process.", index = 0)
  var genotypes: String = null

  @Argument(required = true, metaVar = "PHENOTYPES", usage = "The phenotypes to process.", index = 1)
  var phenotypes: String = null

  @Argument(required = true, metaVar = "ASSOCIATION_TYPE", usage = "The type of association to run. Options are CHI_SQUARED, ADDITIVE_LINEAR, ADDITIVE_LOGISTIC, DOMINANT_LINEAR, DOMINANT_LOGISTIC", index = 2)
  var associationType: String = null

  @Argument(required = true, metaVar = "ASSOCIATIONS", usage = "The location to save associations to.", index = 3)
  var associations: String = null

  @Args4jOption(required = false, name = "-phenoName", usage = "The phenotype to regress.") // need to have check for this flag somewhere if the associaiton type is on of the multiple regressions.
  var phenoName: String = null

  @Args4jOption(required = false, name = "-covar", usage = "Whether to include covariates.") // this will be used to construct the original phenotypes array in LoadPhenotypes.
  var includeCovariates = false

  @Args4jOption(required = false, name = "-covarFile", usage = "The covariates file path")
  var covarFile: String = null

  @Args4jOption(required = false, name = "-covarNames", usage = "The covariates to include in the analysis") // this will be used to construct the original phenotypes array in LoadPhenotypes. Will need to throw out samples that don't have all of the right fields.
  var covarNames: String = null

  @Args4jOption(required = false, name = "-regions", usage = "The regions to filter genotypes by.")
  var regions: String = null

  @Args4jOption(required = false, name = "-saveAsText", usage = "Chooses to save as text. If not selected, saves to Parquet.")
  var saveAsText = false

  @Args4jOption(required = false, name = "-validationStringency", usage = "The level of validation to use on inputs. By default, strict. Choices are STRICT, LENIENT, SILENT.")
  var validationStringency: String = "STRICT"

  @Args4jOption(required = false, name = "-ploidy", usage = "Ploidy to assume. Default value is 2 (diploid).")
  var ploidy = 2

  @Args4jOption(required = false, name = "-overwriteParquet", usage = "Overwrite parquet file that was created in the vcf conversion.")
  var overwrite = false

  @Args4jOption(required = false, name = "-maf", usage = "Missingness per individual threshold. Default value is 0.01.")
  var maf = 0.01

  @Args4jOption(required = false, name = "-mind", usage = "Missingness per marker threshold. Default value is 0.1.")
  var mind = 0.1

  @Args4jOption(required = false, name = "-geno", usage = "Allele frequency threshold. Default value is 0.")
  var geno = 0

  @Args4jOption(required = false, name = "-oneTwo", usage = "If cases are 1 and controls 2 instead of 0 and 1")
  var oneTwo = false
  //
  //  @Args4jOption(required = false, name = "-mapFile", usage = "Path to PLINK MAP file from which to get Varinat IDs.")
  //  var mapFile: String = null
}

class RegressPhenotypes(protected val args: RegressPhenotypesArgs) extends BDGSparkCommand[RegressPhenotypesArgs] {
  val companion = RegressPhenotypes

  def run(sc: SparkContext) {
    val a = args.oneTwo
    //    println(s"\n\n\n\n\n\n oneTwo: $a \n\n\n\n\n\n\n\n")

    // Load in genotype data
    val genotypeStates = loadGenotypes(sc)

    // Load in phenotype data
    val phenotypes = loadPhenotypes(sc)

    // Perform analysis
    val associations = performAnalysis(genotypeStates, phenotypes, sc)

    // Log the results
    logResults(associations, sc)
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
    //    if (args.getIds) {
    //      val mapPath = args.mapFile
    //      val oldName = new File(args.genotypes).getAbsolutePath.split("/").reverse(0)
    //      val newVCFPath = new File(args.genotypes).getAbsolutePath.split("/").reverse.drop(1).reverse.mkString("/") + "withIds_" + oldName
    //      val outpath = newVCFPath
    //      GetVariantIds(sc, vcfPath)
    //      vcfPath = outpath
    //    }

    import Timers._

    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
    if (!parquetFiles.getAbsoluteFile.exists) {
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      VCF2AdamFunctionTimer.time {
        Vcf2ADAM(cmdLine).run(sc)
      }
    } else if (args.overwrite) {
      FileUtils.deleteDirectory(parquetFiles)
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      VCF2AdamFunctionTimer.time {
        Vcf2ADAM(cmdLine).run(sc)
      }
    }

    // Check for the genotypes file first.
    // val genoFile = new File(args.genotypes)
    // assert(genoFile.exists, "Path to genotypes VCF file is incorrect or the file is missing.")
    //    val absAssociationsPath = args.associations
    //    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    //    val parquetInputDestination = absAssociationsPath.toString.split("/").reverse.drop(1).reverse.mkString("/") + "/parquetFiles/"
    //    val parquetPath = new Path(parquetInputDestination)
    //    val genoPath = new Path(args.genotypes)
    //    if (!hdfs.exists(genoPath)) {
    //      throw new FileNotFoundException(s"Could not find filesystem for ${args.genotypes} with Hadoop configuration ${sc.hadoopConfiguration}")
    //    }
    //
    //    // val absAssociationPath = new Path(args.associations)
    //    // val fs =
    //    //   Option(absAssociationPath.getFileSystem(sc.hadoopConfiguration))
    //    //     .getOrElse(throw new FileNotFoundException(
    //    //       s"Couldn't find filesystem for ${absAssociationPath.toUri} with Hadoop configuration ${sc.hadoopConfiguration}"))
    //    // var parquetInputDestination = absAssociationPath.toString.split("/").reverse.drop(1).reverse.mkString("/") + "/parquetFiles/"
    //    // val parquetFiles = new File(parquetInputDestination)
    //
    //    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
    //    if (!hdfs.exists(parquetPath)) {
    //      val cmdLine: Array[String] = Array[String](args.genotypes, parquetInputDestination)
    //      Vcf2ADAM(cmdLine).run(sc)
    //    } else if (args.overwrite) {
    //      hdfs.delete(parquetPath, true)
    //      //      FileUtils.deleteDirectory(parquetFiles)
    //      val cmdLine: Array[String] = Array[String](args.genotypes, parquetInputDestination)
    //      Vcf2ADAM(cmdLine).run(sc)
    //    }

    // // println(args.associations)
    // val absAssociationPath = args.associations //new File(args.associations) //.getAbsolutePath()
    // println(absAssociationPath)
    // // println(absAssociationPath)
    // // var parquetInputDestination = absAssociationPath.split("/").reverse.drop(1).reverse.mkString("/")
    // var parquetInputDestination = absAssociationPath + "/parquetFiles/"
    // println(parquetInputDestination)
    // // println(parquetInputDestination)
    // // parquetInputDestination = parquetInputDestination + "/parquetInputFiles/"
    // // println(parquetInputDestination)
    // val parquetFiles = new File(parquetInputDestination)
    // println(parquetInputDestination)
    // println(parquetFiles.exists)
    // println(parquetFiles.getAbsoluteFile)
    // println(parquetFiles.getAbsoluteFile.exists)

    // println("Genotypes? ")
    // val a = new File(args.genotypes)
    // println(a.exists)

    // /* Check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet
    //  using vcf2adam.
    // */
    // // if (!parquetFiles.getAbsoluteFile().exists) {
    // if (!parquetFiles.exists) {
    //   val cmdLine: Array[String] = Array[String](args.genotypes, parquetInputDestination)
    //   Vcf2ADAM(cmdLine).run(sc)
    //   println(parquetFiles.getAbsoluteFile().exists)
    // } else if (args.overwrite) {
    //   FileUtils.deleteDirectory(parquetFiles)
    //   val cmdLine: Array[String] = Array[String](args.genotypes, parquetInputDestination)
    //   Vcf2ADAM(cmdLine).run(sc)
    // }
    //    val parquetInputDestination = args.genotypes
    // read in parquet files
    import sqlContext.implicits._
    //    val genotypes = sqlContext.read.parquet(parquetInputDestination)
    val genotypes = sqlContext.read.format("parquet").load(parquetInputDestination)
    //    val genotypes = sc.loadGenotypes(parquetInputDestination).toDF()
    // transform the parquet-formatted genotypes into a dataFrame of GenotypeStates and convert to Dataset.

    val genotypeStates =
      ParquetToDataFrameTimer.time { sqlContext.toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = false) }

    val genoStatesWithNames = genotypeStates.select(concat($"contig", lit("_"), $"end", lit("_"), $"alt") as "contig",
      genotypeStates("start"),
      genotypeStates("end"),
      genotypeStates("ref"),
      genotypeStates("alt"),
      genotypeStates("sampleId"),
      genotypeStates("genotypeState"),
      genotypeStates("missingGenotypes"))
    println(genoStatesWithNames.take(10).toList)

    /*
    For now, just going to use PLINK's Filtering functionality to create already-filtered vcfs from the BED.
    TODO: Write genotype filters for missingness, MAF, and genotyping rate
      - To do the filtering, create a genotypeState matrix and calculate which SNPs and Samples need to be filtered out.
      - Then go back into the Dataset of GenotypeStates and filter out those GenotypeStates.
    */

    // convert to genotypestatematrix dataframe
    //
    // .as[GenotypeState]

    // apply filters: MAF, genotyping rate; (throw out SNPs with low MAF or genotyping rate)
    // genotypeStates.registerTempTable("genotypes")
    // val filteredGenotypeStates = sqlContext.sql("SELECT * FROM genotypes WHERE ")

    // apply filters: missingness; (throw out samples missing too many SNPs)
    // val finalGenotypeStates =

    // mind filter
    genoStatesWithNames.registerTempTable("genotypeStates")

    val mindDF =
      MindDataFrameFilterTimer.time {
        sqlContext.sql("SELECT sampleId FROM genotypeStates GROUP BY sampleId HAVING SUM(missingGenotypes)/(COUNT(sampleId)*2) <= %s".format(args.mind))
      }

    // TODO: Resolve with "IN" sql command once spark2.0 is integrated
    val filteredGenotypeStates =
      GenoDataFrameFilterTimer.time { genotypeStates.filter(($"sampleId").isin(mindDF.collect().map(r => r(0)): _*)) }
    filteredGenotypeStates.as[GenotypeState]
  }

  def loadPhenotypes(sc: SparkContext): RDD[Phenotype[Array[Double]]] = {
    // """
    // SNPs and Samples are filtered out by PLINK when creating the VCF file.
    // """
    if (args.associationType == "CHI_SQUARED") {
      assert(false, "CHI_SQUARED analysis has been phased out.")
    }
    // assert that a phenoName is given
    assert(Option[String](args.phenoName).isDefined, "The model assumes a phenotype file with multiple phenotypes as columns and a phenoName must be given.")

    // assert covariates are given if -covar given
    if (args.includeCovariates) {
      assert(Option[String](args.covarNames).isDefined, "If the -covar flag is given, covariate names must be given using the -covarNames flag")
      // assert that the primary phenotype isn't included in the covariates.
      for (covar <- args.covarNames.split(",")) {
        assert(covar != args.phenoName, "Primary phenotype cannot be a covariate.")
      }
    }

    import Timers._

    // Load phenotypes
    var phenotypes: RDD[Phenotype[Array[Double]]] = null
    if (args.includeCovariates) {
      LoadPhenotypeTimer.time {
        phenotypes = LoadPhenotypesWithCovariates(args.oneTwo, args.phenotypes, args.covarFile, args.phenoName, args.covarNames, sc)
      }
    } else {
      LoadPhenotypeTimer.time {
        phenotypes = LoadPhenotypesWithoutCovariates(args.oneTwo, args.phenotypes, args.phenoName, sc)
      }
    }
    phenotypes
  }

  def performAnalysis(genotypeStates: Dataset[GenotypeState],
                      phenotypes: RDD[Phenotype[Array[Double]]],
                      sc: SparkContext): Dataset[Association] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    val contextOption = Option(sc)

    import AuxEncoders._
    import org.apache.spark.rdd.MetricsContext._

    val RDDgenotypeStates: RDD[GenotypeState] = genotypeStates.rdd.instrument()

    val associations = args.associationType match {
      case "ADDITIVE_LINEAR"   => AdditiveLinearAssociation(RDDgenotypeStates, phenotypes)
      case "ADDITIVE_LOGISTIC" => AdditiveLogisticAssociation(RDDgenotypeStates, phenotypes)
      case "DOMINANT_LINEAR"   => DominantLinearAssociation(RDDgenotypeStates, phenotypes)
      case "DOMINANT_LOGISTIC" => DominantLogisticAssociation(RDDgenotypeStates, phenotypes)
    }
    //    associations.take(100).foreach(assoc => println(assoc))
    sqlContext.createDataset(associations)
  }

  def logResults(associations: Dataset[Association],
                 sc: SparkContext) = {
    // save dataset
    val sqlContext = SQLContext.getOrCreate(sc)
    val associationsFile = new File(args.associations)
    if (associationsFile.exists) {
      FileUtils.deleteDirectory(associationsFile)
    }
    if (args.saveAsText) {
      associations.rdd.keyBy(_.logPValue).sortBy(_._1).map(r => "%s, %s, %s"
        .format(r._2.variant.getContig.getContigName,
          r._2.variant.getStart, Math.pow(10, r._2.logPValue).toString))
        .saveAsTextFile(args.associations)
    } else {
      associations.toDF.write.parquet(args.associations)
    }
  }
}

object Timers extends Metrics {
  val VCF2AdamFunctionTimer = timer("Convert VCF to Adam format")
  val ParquetToDataFrameTimer = timer("Loading Parquet File to Data Frame")
  val MindDataFrameFilterTimer = timer("MinD dataframe filter operation")
  val GenoDataFrameFilterTimer = timer("Geno dataframe filter operation")
  val LoadPhenotypeTimer = timer("Load Phenotype operation")
}