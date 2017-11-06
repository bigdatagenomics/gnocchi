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
package org.bdgenomics.gnocchi.cli

import org.bdgenomics.gnocchi.algorithms.siteregression._
import org.bdgenomics.gnocchi.models.variant.{ LinearVariantModel, LogisticVariantModel, VariantModel }
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

import scala.io.StdIn.readLine

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

  @Argument(required = true, metaVar = "OUTPUT", usage = "The location to save associations to.", index = 3)
  var output: String = _

  @Args4jOption(required = false, name = "-sampleIDName", usage = "The name of the column containing unique ID's for samples")
  var sampleUID: String = _

  @Args4jOption(required = false, name = "-phenoName", usage = "The phenotype to regress.")
  var phenoName: String = _

  @Args4jOption(required = false, name = "-phenoSpaceDelimited", usage = "Set flag if phenotypes file is space delimited, otherwise tab delimited is assumed.")
  var phenoSpaceDelimiter = false

  @Args4jOption(required = false, name = "-covar", usage = "Whether to include covariates.")
  var includeCovariates = false

  @Args4jOption(required = false, name = "-covarFile", usage = "The covariates file path")
  var covarFile: String = _

  @Args4jOption(required = false, name = "-covarNames", usage = "The covariates to include in the analysis") // this will be used to construct the original phenotypes array in LoadPhenotypes. Will need to throw out samples that don't have all of the right fields.
  var covarNames: String = _

  @Args4jOption(required = false, name = "-covarSpaceDelimited", usage = "Set flag if covariates file is space delimited, otherwise tab delimited is assumed.")
  var covarSpaceDelimiter = false

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

  @Args4jOption(required = false, name = "-geno", usage = "Missingness per marker threshold. Default value is 0.1.")
  var geno = 0.1

  @Args4jOption(required = false, name = "-oneTwo", usage = "If cases are 1 and controls 2 instead of 0 and 1")
  var oneTwo = false

  @Args4jOption(required = false, name = "-forceSave", usage = "If set to true, no prompt will be given and results will overwrite any other files at that location.")
  var forceSave = false
}

class RegressPhenotypes(protected val args: RegressPhenotypesArgs) extends BDGSparkCommand[RegressPhenotypesArgs] {
  val companion = RegressPhenotypes

  def run(sc: SparkContext) {

    val phenoDelimiter = if (args.phenoSpaceDelimiter) { " " } else { "\t" }
    val covarDelimiter = if (args.covarSpaceDelimiter) { " " } else { "\t" }
    val missingPhenos = if (args.oneTwo) List(0, -9) else List(-9)

    val phenotypes = if (args.covarFile != null) {
      sc.loadPhenotypes(args.phenotypes,
        args.sampleUID,
        args.phenoName,
        phenoDelimiter,
        Option(args.covarFile),
        Option(args.covarNames.split(",").toList),
        covarDelimiter = covarDelimiter,
        missing = missingPhenos)
    } else {
      sc.loadPhenotypes(args.phenotypes, args.sampleUID, args.phenoName, phenoDelimiter, missing = missingPhenos)
    }
    val broadPhenotype = sc.broadcast(phenotypes)

    val rawGenotypes = sc.loadGenotypes(args.genotypes)
    // val recoded = sc.recodeMajorAllele(rawGenotypes)
    val sampleFiltered = sc.filterSamples(rawGenotypes, mind = args.mind, ploidy = args.ploidy)
    val filteredGeno = sc.filterVariants(sampleFiltered, geno = args.geno, maf = args.maf)

    args.associationType match {
      case "ADDITIVE_LINEAR" =>
        val associations = LinearSiteRegression(filteredGeno, broadPhenotype, "ADDITIVE")
        sc.saveAssociations[LinearVariantModel](associations, args.output, args.saveAsText, args.forceSave)
      case "DOMINANT_LINEAR" =>
        val associations = LinearSiteRegression(filteredGeno, broadPhenotype, "DOMINANT")
        sc.saveAssociations[LinearVariantModel](associations, args.output, args.saveAsText, args.forceSave)
      case "ADDITIVE_LOGISTIC" =>
        val associations = LogisticSiteRegression(filteredGeno, broadPhenotype, "ADDITIVE")
        sc.saveAssociations[LogisticVariantModel](associations, args.output, args.saveAsText, args.forceSave)
      case "DOMINANT_LOGISTIC" =>
        val associations = LogisticSiteRegression(filteredGeno, broadPhenotype, "DOMINANT")
        sc.saveAssociations[LogisticVariantModel](associations, args.output, args.saveAsText, args.forceSave)
    }
  }
}
