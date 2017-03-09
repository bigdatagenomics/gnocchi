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

import net.fnothaft.gnocchi.models.GenotypeState
import net.fnothaft.gnocchi.similarity.SampleSimilarity
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object ComputeSampleSimilarity extends BDGCommandCompanion {
  val commandName = "sampleSimilarity"
  val commandDescription = "Computes the pairwise genotype similarity of all samples."

  def apply(cmdLine: Array[String]) = {
    new ComputeSampleSimilarity(Args4j[ComputeSampleSimilarityArgs](cmdLine))
  }
}

class ComputeSampleSimilarityArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The genotypes to process.", index = 0)
  var input: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "The sample similarity.", index = 1)
  var output: String = null

  @Args4jOption(required = false, name = "-similarityThreshold",
    usage = "The minimum sample similarity to estimate. Default value is 0.5.")
  var similarityThreshold = 0.5

  @Args4jOption(required = false, name = "-saveAsText", usage = "Chooses to save as text. If not selected, saves to Parquet.")
  var saveAsText = false

  @Args4jOption(required = false, name = "-ploidy", usage = "Ploidy to assume. Default value is 2 (diploid).")
  var ploidy = 2
}

class ComputeSampleSimilarity(protected val args: ComputeSampleSimilarityArgs) extends BDGSparkCommand[ComputeSampleSimilarityArgs] {
  val companion = ComputeSampleSimilarity

  def run(sc: SparkContext) {
    // load in genotype data
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val genotypes = sqlContext.read
      .format("parquet")
      .load(args.input)
    val genotypeStates = sqlContext.toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = true)
      .as[GenotypeState]

    // compute similarity
    val similarities = SampleSimilarity(genotypeStates, args.similarityThreshold)

    // save to disk
    val format = if (args.saveAsText) {
      "json"
    } else {
      "parquet"
    }
    sqlContext.createDataFrame(similarities)
      .write
      .format(format)
      .save(args.output)
  }
}
