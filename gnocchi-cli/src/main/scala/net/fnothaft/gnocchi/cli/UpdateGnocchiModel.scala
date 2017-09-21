///**
// * Licensed to Big Data Genomics (BDG) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The BDG licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package net.fnothaft.gnocchi.cli
//
//import java.io.FileInputStream
//
//import net.fnothaft.gnocchi.models.GnocchiModelMetaData
//import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
//import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
//import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
//import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
//import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
//import org.apache.spark.SparkContext
//import org.bdgenomics.utils.cli._
//import org.kohsuke.args4j.{ Option => Args4jOption }
//import net.fnothaft.gnocchi.sql.GnocchiContext._
//import org.apache.spark.sql.SparkSession
//
//object UpdateGnocchiModel extends BDGCommandCompanion {
//  val commandName = "UpdateGnocchiModel"
//  val commandDescription = "Updates saved GnocchiModel with new batch of data"
//
//  def apply(cmdLine: Array[String]) = {
//    new UpdateGnocchiModel(Args4j[UpdateGnocchiModelArgs](cmdLine))
//  }
//}
//
//class UpdateGnocchiModelArgs extends RegressPhenotypesArgs {
//
//  @Args4jOption(required = true, name = "-modelLocation", usage = "The location of the model to load.")
//  var modelLocation: String = _
//
//  @Args4jOption(required = true, name = "-saveModelTo", usage = "The location to save model to.")
//  var saveTo: String = _
//
//}
//
//class UpdateGnocchiModel(protected val args: UpdateGnocchiModelArgs) extends BDGSparkCommand[UpdateGnocchiModelArgs] {
//  override val companion = UpdateGnocchiModel
//
//  override def run(sc: SparkContext) {
//
//    // Load in genotype data filtering out any SNPs not provided in command line
//    val batchGenotypeStates = sc.loadAndFilterGenotypes(args.genotypes, args.associations,
//      args.ploidy, args.mind, args.maf, args.geno, args.overwrite)
//
//    // Load in phenotype data
//    val batchPhenotypes = sc.loadPhenotypes(args.phenotypes, args.phenoName, args.oneTwo,
//      args.includeCovariates, Option(args.covarFile), Option(args.covarNames))
//
//    val model = sc.loadGnocchiModel(args.modelLocation)
//
//    val batchObservations = sc.generateObservations(batchGenotypeStates, batchPhenotypes)
//
//    val updatedModel = model.update(batchObservations)
//
//    // save the model
//    updatedModel.save(args.saveTo)
//  }
//}
