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

import java.io.File

import net.fnothaft.gnocchi.algorithms._
import net.fnothaft.gnocchi.algorithms.siteregression._
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import scala.math.exp
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ concat, lit }
import net.fnothaft.gnocchi.rdd.association._
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import org.apache.hadoop.fs.Path
import net.fnothaft.gnocchi.sql.AuxEncoders

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

  @Args4jOption(required = false, name = "-phenoName", usage = "The phenotype to regress.")
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

    val sparkSession = SparkSession.builder().getOrCreate()

    val genotypeStates = sc.loadAndFilterGenotypes(args.genotypes, args.associations,
      args.ploidy, args.mind, args.maf, args.geno, args.overwrite)

    val phenotypes = sc.loadPhenotypes(args.phenotypes, args.phenoName, args.oneTwo,
      args.includeCovariates, Option(args.covarFile), Option(args.covarNames))

    import net.fnothaft.gnocchi.sql.AuxEncoders._

    args.associationType match {
      case "ADDITIVE_LINEAR" => {
        val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, AdditiveLinearRegression.clipOrKeepState)
        val associations = AdditiveLinearRegression(genoPhenoObs)
        val assocsDS = sparkSession.createDataset(associations.asInstanceOf[RDD[Association[AdditiveLinearVariantModel]]])
        logResults[AdditiveLinearVariantModel](assocsDS, sc)
      }
      case "DOMINANT_LINEAR" => {
        val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, DominantLinearRegression.clipOrKeepState)
        val associations = DominantLinearRegression(genoPhenoObs)
        val assocsDS = sparkSession.createDataset(associations.asInstanceOf[RDD[Association[AdditiveLinearVariantModel]]])
        logResults[AdditiveLinearVariantModel](assocsDS, sc)
      }
      case "ADDITIVE_LOGISTIC" => {
        val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, AdditiveLogisticRegression.clipOrKeepState)
        val associations = AdditiveLogisticRegression(genoPhenoObs)
        val assocsDS = sparkSession.createDataset(associations.asInstanceOf[RDD[Association[AdditiveLogisticVariantModel]]])
        logResults[AdditiveLogisticVariantModel](assocsDS, sc)
      }
      case "DOMINANT_LOGISTIC" => {
        val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, DominantLogisticRegression.clipOrKeepState)
        val associations = DominantLogisticRegression(genoPhenoObs)
        val assocsDS = sparkSession.createDataset(associations.asInstanceOf[RDD[Association[AdditiveLogisticVariantModel]]])
        logResults[AdditiveLogisticVariantModel](assocsDS, sc)
      }
    }
  }

  def logResults[A <: VariantModel[A]](associations: Dataset[Association[A]],
                                       sc: SparkContext) = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    // save dataset
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
