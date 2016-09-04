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

import htsjdk.samtools.ValidationStringency
import java.io.File
import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.BroadcastRegionJoin
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.apache.parquet.avro.AvroReadSupport
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.util.ContextUtil

import org.bdgenomics.adam.cli.Vcf2ADAM
import java.nio.file.{ Paths, Files }
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset }
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
}

class RegressPhenotypes(protected val args: RegressPhenotypesArgs) extends BDGSparkCommand[RegressPhenotypesArgs] {
  val companion = RegressPhenotypes

  def run(sc: SparkContext) {

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

    val absAssociationPath = new File(args.associations).getAbsolutePath()
    var parquetInputDestination = absAssociationPath.split("/").reverse.drop(1).reverse.mkString("/")
    parquetInputDestination = parquetInputDestination + "/parquetInputFiles/"
    val parquetFiles = new File(parquetInputDestination)

    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
    if (!parquetFiles.getAbsoluteFile().exists) {
      val cmdLine: Array[String] = Array[String](args.genotypes, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    } else if (args.overwrite) {
      FileUtils.deleteDirectory(parquetFiles)
      val cmdLine: Array[String] = Array[String](args.genotypes, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    }

    // read in parquet files
    import sqlContext.implicits._
    val genotypes = sqlContext.read.parquet(parquetInputDestination)

    // transform the parquet-formatted genotypes into a dataFrame of GenotypeStates and convert to Dataset.
    val genotypeStates = sqlContext.toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = false)
      .as[GenotypeState]

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

    // return genotypeStates
    return genotypeStates

  }

  def loadPhenotypes(sc: SparkContext): RDD[Phenotype[Array[Double]]] = {
    // """ 
    // SNPs and Samples are filtered out by PLINK when creating the VCF file. 
    // """
    if (args.associationType == "CHI_SQUARED") {
      assert(false, "CHI_SQUARED has been phased out.")
    }
    // assert that a phenoName is given
    assert(Option[String](args.phenoName).isDefined, "The model assumes a phenotype file with multiple phenotypes as columns and a phenoName must be given.")

    // assert covariates are given if -covar given
    if (args.includeCovariates) {
      assert(Option[String](args.covarNames).isDefined, "If the -covar flag is given, covarite names must be given using the -covarNames flag")
      // assert that the primary phenotype isn't included in the covariates. 
      for (covar <- args.covarNames.split(",")) {
        assert(covar != args.phenoName, "Primary phenotype cannot be a covariate.")
      }
    }

    // Load phenotypes
    var phenotypes: RDD[Phenotype[Array[Double]]] = null
    if (args.includeCovariates) {
      phenotypes = LoadPhenotypesWithCovariates(args.phenotypes, args.phenoName, args.covarNames, sc)
    } else {
      phenotypes = LoadPhenotypesWithoutCovariates(args.phenotypes, args.phenoName, sc)
    }
    return phenotypes
  }

  def performAnalysis(genotypeStates: Dataset[GenotypeState],
                      phenotypes: RDD[Phenotype[Array[Double]]],
                      sc: SparkContext): Dataset[Association] = {
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    import AuxEncoders._
    import org.apache.spark.SparkContext._
    val associations = args.associationType match {
      case "ADDITIVE_LINEAR" => AdditiveLinearAssociation(genotypeStates.rdd, phenotypes)
      // case "ADDITIVE_LOGISTIC" => assert(false, "Logistic regression is not implemented yet")
      case "DOMINANT_LINEAR" => DominantLinearAssociation(genotypeStates.rdd, phenotypes)
      // case "DOMINANT_LOGISTIC" => assert(false, "Logistic regression is not implemented yet")
    }
    return sqlContext.createDataset(associations)
  }

  def logResults(associations: Dataset[Association],
                 sc: SparkContext) = {
    // save dataset
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val associationsFile = new File(args.associations)
    if (associationsFile.exists) {
      FileUtils.deleteDirectory(associationsFile)
    }
    if (args.saveAsText) {
      // associations.toDF.rdd.map(_.toString).saveAsTextFile(args.associations)
      associations.rdd.saveAsTextFile(args.associations)
    } else {
      associations.toDF.write.parquet(args.associations)
    }
  }
}
