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

import org.apache.spark.SparkContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }
//import net.fnothaft.gnocchi.association.Ensembler TODO: pull in the ensembler code or predict won't work.

object UpdateGnocchiModel extends BDGCommandCompanion {
  val commandName = "UpdateGnocchiModel"
  val commandDescription = "Fill this out later!!"

  def apply(cmdLine: Array[String]) = {
    new UpdateGnocchiModel(Args4j[UpdateGnocchiModelArgs](cmdLine))
  }
}

class UpdateGnocchiModelArgs extends RegressPhenotypesArgs {

  @Args4jOption(required = true, name = "-modelLocation", usage = "The location of the model to load.")
  var modelLocation: String = _

  @Args4jOption(required = true, name = "-saveModelTo", usage = "The location to save model to.")
  var saveTo: String = _

}

class UpdateGnocchiModel(protected val args: UpdateGnocchiModelArgs) extends BDGSparkCommand[UpdateGnocchiModelArgs] {
  override val companion = UpdateGnocchiModel

  override def run(sc: SparkContext) {

    // instantiate regressPhenotypes obj
    val regPheno = new RegressPhenotypes(args)

    // Load in genotype data filtering out any SNPs not provided in command line
    val genotypeStates = regPheno.loadGenotypes(sc)

    // Load in phenotype data
    val phenotypes = regPheno.loadPhenotypes(sc)

    // load model
    val model = LoadGnocchiModel(args.modelLocation)

    // update the model with new data
    model.update(genotypeStates, phenotypes, sc)

    // save the model
    SaveGnocchiModel(model, args.saveTo)

  }

}
