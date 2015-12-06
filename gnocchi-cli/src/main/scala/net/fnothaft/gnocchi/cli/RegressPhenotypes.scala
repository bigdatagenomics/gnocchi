/**
 * Copyright 2015 Frank Austin Nothaft
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
import org.apache.spark.{ Logging, SparkContext }
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

  @Argument(required = true, metaVar = "ASSOCIATIONS", usage = "The location to save associations to.", index = 2)
  var associations: String = null

  @Args4jOption(required = false, name = "-regions", usage = "The regions to filter genotypes by.")
  var regions: String = null

  @Args4jOption(required = false, name = "-saveAsText", usage = "Chooses to save as text. If not selected, saves to Parquet.")
  var saveAsText = false

  @Args4jOption(required = false, name = "-validationStringency", usage = "The level of validation to use on inputs. By default, strict. Choices are STRICT, LENIENT, SILENT.")
  var validationStringency: String = "STRICT"

  @Args4jOption(required = false, name = "-ploidy", usage = "Ploidy to assume. Default value is 2 (diploid).")
  var ploidy = 2
}

class RegressPhenotypes(protected val args: RegressPhenotypesArgs) extends BDGSparkCommand[RegressPhenotypesArgs] {
  val companion = RegressPhenotypes

  def run(sc: SparkContext) {
    // load in genotype data
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val genotypes = sqlContext.read
      .format("parquet")
      .load(args.genotypes)
    val genotypeStates = sqlContext.toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = false)
      .as[GenotypeState]

    // load in phenotype data
    val phenotypes = LoadPhenotypes[Boolean](args.phenotypes, sc)

    // validate genotypes and phenotypes
    LoadPhenotypes.validate(phenotypes,
                            genotypeStates,
                            ValidationStringency.valueOf(args.validationStringency))

    // score associations
    val associations = ScoreAssociation(genotypeStates, phenotypes)

    // save dataset
    if (args.saveAsText) {
      associations.rdd
        .map(_.toString)
        .saveAsTextFile(args.associations)
    } else {
      associations.toDF.write.parquet(args.associations)
    }
  }
}
