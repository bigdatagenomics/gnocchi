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

import net.fnothaft.gnocchi.models.{ GenotypeState, ReducedDimension }
import net.fnothaft.gnocchi.clustering.WideFlatPCA
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object ReduceDimensions extends BDGCommandCompanion {
  val commandName = "reduceDimensions"
  val commandDescription = "Reduces the dimensionality of all genotypes."

  def apply(cmdLine: Array[String]) = {
    new ReduceDimensions(Args4j[ReduceDimensionsArgs](cmdLine))
  }
}

class ReduceDimensionsArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "The genotypes to process.", index = 0)
  var input: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "The reduced dimensions for all samples.", index = 1)
  var output: String = null

  @Argument(required = true, metaVar = "DIMENSIONS", usage = "The number of dimensions to reduce down to.", index = 2)
  var dimensions: Int = -1

  @Args4jOption(required = false, name = "-saveAsText", usage = "Chooses to save as text. If not selected, saves to Parquet.")
  var saveAsText = false

  @Args4jOption(required = false, name = "-ploidy", usage = "Ploidy to assume. Default value is 2 (diploid).")
  var ploidy = 2
}

class ReduceDimensions(protected val args: ReduceDimensionsArgs) extends BDGSparkCommand[ReduceDimensionsArgs] {
  val companion = ReduceDimensions

  def run(sc: SparkContext) {
    require(args.dimensions >= 1, "Dimensionality (%d) must be positive.".format(args.dimensions))

    // load in genotype data
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    val genotypes = sqlContext.read
      .format("parquet")
      .load(args.input)
    val genotypeStates = sqlContext.toGenotypeStateDataFrame(genotypes, args.ploidy, sparse = true)
      .as[GenotypeState]

    // compute similarity
    val dimensions = WideFlatPCA(genotypeStates, args.dimensions)

    // save to disk
    val format = if (args.saveAsText) {
      "json"
    } else {
      "parquet"
    }
    dimensions.toDF()
      .write
      .format(format)
      .save(args.output)
  }
}
