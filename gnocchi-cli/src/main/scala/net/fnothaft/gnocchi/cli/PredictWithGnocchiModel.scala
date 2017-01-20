/**
 * Copyright 2016 Taner Dagdelen
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

object PredictWithGnocchiModel extends BDGCommandCompanion {
  val commandName = "PredictWithGnocchiModel"
  val commandDescription = "Fill this out later!!"

  def apply(cmdLine: Array[String]) = {
    new PredictWithGnocchiModel(Args4j[PredictWithGnocchiModelArgs](cmdLine))
  }
}

class PredictWithGnocchiModelArgs extends RegressPhenotypesArgs {

  @Args4jOption(required = true, name = "-modelLocation", usage = "The location of the model to load.")
  var modelLocation: String = _

  @Args4jOption(required = true, name = "-savePredictionsTo", usage = "The location to save predictions to.")
  var saveTo: String = _

}

class PredictWithGnocchiModel(protected val args: PredictWithGnocchiModelArgs) extends BDGSparkCommand[PredictWithGnocchiModelArgs] {
  override val companion = PredictWithGnocchiModel

  override def run(sc: SparkContext) {

    // instantiate regressPhenotypes obj
    val regPheno = new RegressPhenotypes(args)

    // Load in genotype data filtering out any SNPs not provided in command line
    val genotypeStates = regPheno.loadGenotypes(sc)

    // Load in phenotype data
    val phenotypes = regPheno.loadPhenotypes(sc)

    // load model TODO: Write load GnocchiModel object
    val model = LoadGnocchiModel(args.modelLocation)

    // make predictions on new data
    val predictions = model.predict(genotypeStates.rdd, phenotypes, sc)

    // save the predictions TODO: write savePredictions function
    //    savePredictions(predictions)

    // save the model
    SaveGnocchiModel(model, args.saveTo)
  }
}
