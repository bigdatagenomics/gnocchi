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

import java.io.FileInputStream

import net.fnothaft.gnocchi.models.{ GnocchiModelMetaData, QualityControlVariant }
import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import org.apache.spark.SparkContext
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.sql.SparkSession

object UpdateGnocchiModel extends BDGCommandCompanion {
  val commandName = "UpdateGnocchiModel"
  val commandDescription = "Updates saved GnocchiModel with new batch of data"

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

    var covarFile: Option[String] = None
    var covarNames: Option[String] = None

    if (args.covarFile != "") {
      covarFile = Option(args.covarFile)
    }

    if (args.covarNames != "") {
      covarNames = Option(args.covarNames)
    }

    // Load in genotype data filtering out any SNPs not provided in command line
    val batchGenotypeStates = sc.loadAndFilterGenotypes(args.genotypes, args.associations,
      args.ploidy, args.mind, args.maf, args.geno, args.overwrite)

    // Load in phenotype data
    val batchPhenotypes = sc.loadPhenotypes(args.phenotypes, args.phenoName, args.oneTwo,
      args.includeCovariates, covarFile, covarNames)

    // sets up sparkSession
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    // load model
    val gnocchiModelPath = args.modelLocation
    val vmLocation = gnocchiModelPath + "/variantModels"
    val qcModelsLocation = gnocchiModelPath + "/qcModels"
    val metaDataLocation = gnocchiModelPath + "/metaData"
    val metaDataIn = new FileInputStream(metaDataLocation)
    val metaData = metaDataIn.read.asInstanceOf[GnocchiModelMetaData]

    val model = args.associationType match {
      case "ADDITIVE_LINEAR" => {
        val variantModels = sparkSession.read.parquet(vmLocation).as[AdditiveLinearVariantModel].rdd
        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariant[AdditiveLinearVariantModel]].rdd
          .map(qcv => {
            (qcv.variantModel, qcv.observations)
          })
        AdditiveLinearGnocchiModel(metaData, variantModels, qcModels)
      }
      case "DOMINANT_LINEAR" => {
        val variantModels = sparkSession.read.parquet(vmLocation).as[DominantLinearVariantModel].rdd
        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariant[DominantLinearVariantModel]].rdd
          .map(qcv => {
            (qcv.variantModel, qcv.observations)
          })
        DominantLinearGnocchiModel(metaData, variantModels, qcModels)
      }
      case "ADDITIVE_LOGISTIC" => {
        val variantModels = sparkSession.read.parquet(vmLocation).as[AdditiveLogisticVariantModel].rdd
        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariant[AdditiveLogisticVariantModel]].rdd
          .map(qcv => {
            (qcv.variantModel, qcv.observations)
          })
        AdditiveLogisticGnocchiModel(metaData, variantModels, qcModels)
      }
      case "DOMINANT_LOGISTIC" => {
        val variantModels = sparkSession.read.parquet(vmLocation).as[DominantLogisticVariantModel].rdd
        val qcModels = sparkSession.read.parquet(qcModelsLocation).as[QualityControlVariant[DominantLogisticVariantModel]].rdd
          .map(qcv => {
            (qcv.variantModel, qcv.observations)
          })
        DominantLogisticGnocchiModel(metaData, variantModels, qcModels)
      }
    }

    val batchObservations = sc.generateObservations(batchGenotypeStates, batchPhenotypes)
    // update the model with new data
    val updatedModel = model.update(batchObservations)

    // save the model
    updatedModel.save(args.saveTo)

  }

}
