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
package net.fnothaft.gnocchi.cli

import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import scala.math.exp
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ concat, lit }
import net.fnothaft.gnocchi.models.{ Association, AuxEncoders, Phenotype }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.sql.functions.{ concat, lit, sum, count, struct, explode, collect_list }
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
  var genotypes: String = _

  @Argument(required = true, metaVar = "PHENOTYPES", usage = "The phenotypes to process.", index = 1)
  var phenotypes: String = _

  @Argument(required = true, metaVar = "ASSOCIATION_TYPE", usage = "The type of association to run. Options are ADDITIVE_LINEAR, ADDITIVE_LOGISTIC, DOMINANT_LINEAR, DOMINANT_LOGISTIC", index = 2)
  var associationType: String = _

  @Argument(required = true, metaVar = "ASSOCIATIONS", usage = "The location to save associations to.", index = 3)
  var associations: String = _

  @Args4jOption(required = true, name = "-phenoName", usage = "The phenotype to regress.")
  var phenoName: String = _

  @Args4jOption(required = false, name = "-covar", usage = "Whether to include covariates.")
  var includeCovariates = false

  @Args4jOption(required = false, name = "-covarFile", usage = "The covariates file path")
  var covarFile: String = _

  @Args4jOption(required = false, name = "-covarNames", usage = "The covariates to include in the analysis") // this will be used to construct the original phenotypes array in LoadPhenotypes. Will need to throw out samples that don't have all of the right fields.
  var covarNames: String = _

  @Args4jOption(required = false, name = "-saveAsText", usage = "Chooses to save as text. If not selected, saves to Parquet.")
  var saveAsText = false

  @Args4jOption(required = false, name = "-validationStringency", usage = "The level of validation to use on inputs. By default, lenient. Choices are STRICT, LENIENT, SILENT.")
  var validationStringency: String = "LENIENT"

  @Args4jOption(required = false, name = "-ploidy", usage = "Ploidy to assume. Default value is 2 (diploid).")
  var ploidy = 2

  @Args4jOption(required = false, name = "-overwriteParquet", usage = "Overwrite parquet file that was created in the vcf conversion.")
  var overwrite = false

  @Args4jOption(required = false, name = "-maf", usage = "Allele frequency threshold. Default value is 0.01.")
  var maf = 0.01

  @Args4jOption(required = false, name = "-mind", usage = "Missingness per individual threshold. Default value is 0.1.")
  var mind = 0.1

  @Args4jOption(required = false, name = "-geno", usage = "Missingness per marker threshold. Default value is 1.")
  var geno = 1.0

  @Args4jOption(required = false, name = "-oneTwo", usage = "If cases are 1 and controls 2 instead of 0 and 1")
  var oneTwo = false

}

class RegressPhenotypes(protected val args: RegressPhenotypesArgs) extends BDGSparkCommand[RegressPhenotypesArgs] {
  val companion = RegressPhenotypes

  def run(sc: SparkContext) {

    val genotypeStates = loadGenotypes(sc)

    val phenotypes = loadPhenotypes(sc)

    val associations = performAnalysis(genotypeStates, phenotypes, sc)

    logResults(associations, sc)
  }

  /**
   * Returns a dataset of GenotypeState's from vcf input.
   *
   * @note Checks for ADAM-formatted (parquet) genotype data in the output
   *       directory. If parquet files don't exist or -overwriteParquet flag
   *       provided, creates ADAM-formatted files from vcf using Vcf2Adam
   *       before creation of dataset.
   * @param sc The spark context in which Gnocchi is running.
   * @return A dataset of GenotypeState objects.
   */
  def loadGenotypes(sc: SparkContext): RDD[GenotypeState] = {
    // sets up sparkSession
    val sparkSession = SparkSession.builder().getOrCreate()

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

    // read in parquet files
    import sparkSession.implicits._
    //    val genotypes = sparkSession.read.parquet(parquetInputDestination)
    val genotypes = sparkSession.read.format("parquet").load(parquetInputDestination)
    //    val genotypes = sc.loadGenotypes(parquetInputDestination).toDF()
    // transform the parquet-formatted genotypes into a dataFrame of GenotypeStates and convert to Dataset.
    val genotypeStates = sparkSession
      .toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = false)
    val genoStatesWithNames = genotypeStates.select(
      concat($"contigName", lit("_"), $"end", lit("_"), $"alt") as "contigName",
      genotypeStates("start"),
      genotypeStates("end"),
      genotypeStates("ref"),
      genotypeStates("alt"),
      genotypeStates("sampleId"),
      genotypeStates("genotypeState"),
      genotypeStates("missingGenotypes"))

    val gsDataset: Dataset[GenotypeState] = genoStatesWithNames.as[GenotypeState]
    val joinImpl = false

    val mindF = sc.broadcast(args.mind)
    val mafF = sc.broadcast(args.maf)
    val genoF = sc.broadcast(args.geno)

    val gsRdd = gsDataset.rdd

    if (joinImpl) {
      val samplesRdd = gsRdd.map(gs => (gs.sampleId, gs.missingGenotypes, 2))
        .keyBy(_._1)
        .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3))
        .map(keyed_tup => {
          val (key, tuple) = keyed_tup
          val (sampleId, missCount, total) = tuple
          val mind = missCount.toDouble / total.toDouble
          (sampleId, mind)
        })
        .filter(_._2 <= mindF.value)

      val sampleFilteredRdd = samplesRdd.join(gsRdd.keyBy(_.sampleId)).map(_._2._2)

      val genosRdd = sampleFilteredRdd.map(gs => (gs.contigName, gs.missingGenotypes, gs.genotypeState, 2))
        .keyBy(_._1)
        .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3, tup1._4 + tup2._4))
        .map(keyed_tup => {
          val (key, tuple) = keyed_tup
          val (contigName, missCount, alleleCount, total) = tuple
          val geno = missCount.toDouble / total.toDouble
          val maf = alleleCount.toDouble / (total - missCount).toDouble
          (contigName, (geno, maf, 1.0 - maf))
        })
        .filter(pair => {
          val (key, stats) = pair
          stats._1 <= genoF.value && stats._2 >= mafF.value && stats._3 >= mafF.value
        })

      val genoFilteredRdd = genosRdd.join(sampleFilteredRdd.keyBy(_.contigName)).map(_._2._2)

      val finalGenotypeStatesRdd = genoFilteredRdd.filter(_.missingGenotypes != 2)

      finalGenotypeStatesRdd
    } else {
      val samples = gsRdd.map(gs => (gs.sampleId, gs.missingGenotypes, 2))
        .keyBy(_._1)
        .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3))
        .map(keyed_tup => {
          val (key, tuple) = keyed_tup
          val (sampleId, missCount, total) = tuple
          val mind = missCount.toDouble / total.toDouble
          (sampleId, mind)
        })
        .filter(_._2 <= mindF.value)
        .map(_._1)
        .collect
        .toSet
      val samples_bc = sc.broadcast(samples)
      val sampleFilteredRdd = gsRdd.filter(gs => samples_bc.value contains gs.sampleId)

      val genos = sampleFilteredRdd.map(gs => (gs.contigName, gs.missingGenotypes, gs.genotypeState, 2))
        .keyBy(_._1)
        .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3, tup1._4 + tup2._4))
        .map(keyed_tup => {
          val (key, tuple) = keyed_tup
          val (contigName, missCount, alleleCount, total) = tuple
          val geno = missCount.toDouble / total.toDouble
          val maf = alleleCount.toDouble / (total - missCount).toDouble
          (contigName, geno, maf, 1.0 - maf)
        })
        .filter(stats => stats._2 <= genoF.value && stats._3 >= mafF.value && stats._4 >= mafF.value)
        .map(_._1)
        .collect
        .toSet
      val genos_bc = sc.broadcast(genos)
      val genoFilteredRdd = sampleFilteredRdd.filter(gs => genos_bc.value contains gs.contigName)

      val finalGenotypeStatesRdd = genoFilteredRdd.filter(_.missingGenotypes != 2)

      finalGenotypeStatesRdd
    }
  }

  /**
   * Returns an RDD of Phenotype objects, built from tab-delimited text file
   * of phenotypes.
   *
   * @throws IllegalArgumentException Throws exception if -includeCovariates
   *                                  given in command line but no covariates
   *                                  specified
   * @throws IllegalArgumentException Throws exception if one of the specified
   *                                  covariates is the name specified in
   *                                  -phenoName
   * @param sc The spark context in which Gnocchi is running.
   * @return An RDD of Phenotype objects.
   */
  def loadPhenotypes(sc: SparkContext): RDD[Phenotype[Array[Double]]] = {
    /*
     * Throws IllegalArgumentException if includeCovariates given but no covariates specified
     * or if any of the covariates specified are the phenotype specified in
     * -phenoName
     */
    if (args.includeCovariates) {
      require(Option[String](args.covarNames).isDefined, "If the -covar flag is given, covariate names must be given using the -covarNames flag")
      // assert that the primary phenotype isn't included in the covariates. 
      for (covar <- args.covarNames.split(",")) {
        assert(covar != args.phenoName, "Primary phenotype cannot be a covariate.")
      }
    }

    // Load phenotypes
    val phenotypes = args.includeCovariates match {
      case true => LoadPhenotypesWithCovariates(args.oneTwo, args.phenotypes, args.covarFile, args.phenoName, args.covarNames, sc)
      case _    => LoadPhenotypesWithoutCovariates(args.oneTwo, args.phenotypes, args.phenoName, sc)
    }
    phenotypes
  }

  /**
   * Returns a dataset of Association objects, each the result of a regression
   * for a single variant.
   *
   * @param genotypeStates The dataset of GenotypeState's created by loadGenotypes
   * @param phenotypes The RDD of Phenotype objects created by loadPhenotypes
   * @param sc: The spark context in which Gnocchi is running.
   * @return A dataset of GenotypeState objects.
   */
  def performAnalysis(genotypeStates: RDD[GenotypeState],
                      phenotypes: RDD[Phenotype[Array[Double]]],
                      sc: SparkContext): Dataset[Association] = {
    // sets up sparkSession
    val sparkSession = SparkSession.builder().getOrCreate()
    val contextOption = Option(sc)

    // imports sparksql encoder for Association objects
    import AuxEncoders._

    // performs regression of given type, producing RDD of association objects
    val associations = args.associationType match {
      case "ADDITIVE_LINEAR"   => AdditiveLinearAssociation(genotypeStates, phenotypes)
      case "ADDITIVE_LOGISTIC" => AdditiveLogisticAssociation(genotypeStates, phenotypes)
      case "DOMINANT_LINEAR"   => DominantLinearAssociation(genotypeStates, phenotypes)
      case "DOMINANT_LOGISTIC" => DominantLogisticAssociation(genotypeStates, phenotypes)
    }

    /*
     * creates dataset of Association objects instead of leaving as RDD in order
     * to make it easy to convert to DataFrame and write to parquet in logResults
     */
    sparkSession.createDataset(associations)
  }

  def logResults(associations: Dataset[Association],
                 sc: SparkContext) = {
    // save dataset
    val sqlContext = SparkSession.builder().getOrCreate()
    val associationsFile = new Path(args.associations)
    val fs = associationsFile.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(associationsFile)) {
      fs.delete(associationsFile, true)
    }

    // enables saving as parquet or human readable text files
    if (args.saveAsText) {
      associations.rdd.keyBy(_.logPValue)
        .sortBy(_._1)
        .map(r => "%s, %s, %s".format(r._2.variant.getContigName,
          r._2.variant.getStart, Math.pow(10, r._2.logPValue).toString))
        .saveAsTextFile(args.associations)
    } else {
      associations.toDF.write.parquet(args.associations)
    }
  }
}
