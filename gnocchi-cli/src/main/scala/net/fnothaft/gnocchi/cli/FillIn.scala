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
import net.fnothaft.gnocchi._
import net.fnothaft.gnocchi.imputation.FillGenotypes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
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

object FillIn extends BDGCommandCompanion {
  val commandName = "fillIn"
  val commandDescription = "Fills in missing genotypes with ref or no call alleles."

  def apply(cmdLine: Array[String]) = {
    new FillIn(Args4j[FillInArgs](cmdLine))
  }
}

class FillInArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The genotypes to process.", index = 0)
  var input: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "The processed genotypes.", index = 1)
  var output: String = null

  @Args4jOption(required = false, name = "-useNoCall",
    usage = "Fills in genotypes with NoCall alleles, instead of Ref.")
  var useNoCall = false

  @Args4jOption(required = false, name = "-ploidy",
    usage = "Assumed ploidy. Default is 2 (diploid).")
  var ploidy = 2
}

class FillIn(protected val args: FillInArgs) extends BDGSparkCommand[FillInArgs] {
  val companion = FillIn

  def run(sc: SparkContext) {
    // load in genotype data
    val genotypes = sc.loadGenotypes(args.input)

    // fill in genotypes
    val recomputedGenotypes = FillGenotypes(genotypes,
      useNoCall = args.useNoCall,
      ploidy = args.ploidy)

    // save to disk
    recomputedGenotypes.adamParquetSave(args.output)
  }
}
