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

import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.gnocchiModel._
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
//import net.fnothaft.gnocchi.association.Ensembler TODO: pull in the ensembler code or predict won't work.
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticGnocchiModel

import scala.collection.mutable.ListBuffer

object ConstructGnocchiModel extends BDGCommandCompanion {
  val commandName = "ConstructGnocchiModel"
  val commandDescription = "Fill this out later!!"

  def apply(cmdLine: Array[String]) = {
    new ConstructGnocchiModel(Args4j[ConstructGnocchiModelArgs](cmdLine))
  }
}

class ConstructGnocchiModelArgs extends RegressPhenotypesArgs {

  @Args4jOption(required = true, name = "-saveModelTo", usage = "The location to save model to.")
  var saveTo: String = _

  @Args4jOption(required = false, name = "SNPS", usage = "The IDs of the SNPs to include in the model, if not all.")
  var snps: String = _
}

class ConstructGnocchiModel(protected val args: ConstructGnocchiModelArgs) extends BDGSparkCommand[ConstructGnocchiModelArgs] {
  override val companion = ConstructGnocchiModel

  override def run(sc: SparkContext): Unit = {

    // instantiate regressPhenotypes obj
    val regPheno = new RegressPhenotypes(args)

    val genotypeStates = regPheno.loadGenotypes(sc)

    // Load in phenotype data
    val phenotypes = regPheno.loadPhenotypes(sc)

    // build model
    val (model, assocs): (GnocchiModel, RDD[Association]) = buildModel[Array[Double]](genotypeStates.rdd, phenotypes, sc)

    // save the associations
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    implicit val associationEncoder = org.apache.spark.sql.Encoders.kryo[Association]
    regPheno.logResults(assocs.toDS, sc)

    // save the model
    SaveGnocchiModel(model, args.saveTo)

  }

  def buildModel[T](genotypeStates: RDD[GenotypeState],
                    phenotypes: RDD[Phenotype[T]],
                    sc: SparkContext): (GnocchiModel, RDD[Association]) = {

    // TODO: finish the other build methods (commented out below)
    val (model, assocs): (GnocchiModel, RDD[Association]) = args.associationType match {
      case "ADDITIVE_LINEAR"   => BuildAdditiveLinearGnocchiModel(genotypeStates, phenotypes, sc)
      case "ADDITIVE_LOGISTIC" => BuildAdditiveLogisticGnocchiModel(genotypeStates, phenotypes, sc)
      //      case "DOMINANT_LINEAR"   => BuildDominantLinearGnocchiModel(genotypeStates, phenotypes, sc)
      //      case "DOMINANT_LOGISTIC" => BuildDominantLogisticGnocchiModel(genotypeStates, phenotypes, sc)
    }
    (model, assocs)
  }

}
