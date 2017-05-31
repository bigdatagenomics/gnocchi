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

import net.fnothaft.gnocchi.algorithms.siteregression.{ AdditiveLinearRegression, AdditiveLogisticRegression, DominantLinearRegression, DominantLogisticRegression }
import net.fnothaft.gnocchi.models.GnocchiModelMetaData
import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Option => Args4jOption }

object BuildGnocchiModel extends BDGCommandCompanion {
  val commandName = "UpdateGnocchiModel"
  val commandDescription = "Updates saved GnocchiModel with new batch of data"

  def apply(cmdLine: Array[String]) = {
    new BuildGnocchiModel(Args4j[BuildGnocchiModelArgs](cmdLine))
  }
}

class BuildGnocchiModelArgs extends RegressPhenotypesArgs {

//  @Args4jOption(required = true, name = "-haplotypeBlocks", usage = "List of variants, one haplotype block per line.")
  //  var haplotypeBlocks: String = _

  @Args4jOption(required = true, name = "-saveModelTo", usage = "The location to save model to.")
  var saveTo: String = _

  @Args4jOption(required = false, name = "-errorThreshold", usage = "Error threshold for flagging models in a haplotype block")
  var errorThreshold: Double = 0.01

}

class BuildGnocchiModel(protected val args: BuildGnocchiModelArgs) extends BDGSparkCommand[BuildGnocchiModel] {
  override val companion = BuildGnocchiModel

  override def run(sc: SparkContext) {

    // Load in genotype data filtering out any SNPs not provided in command line
    val genotypeStates = sc.loadAndFilterGenotypes(args.genotypes, args.associations,
      args.ploidy, args.mind, args.maf, args.geno, args.overwrite)

    // Load in phenotype data
    val phenotypes = sc.loadPhenotypes(args.phenotypes, args.phenoName, args.oneTwo,
      args.includeCovariates, args.covarFile, args.covarNames)

    // Select variant Ids for variants to use as quality control
    val phaseSetsList = sc.extractQCPhaseSetIds(genotypeStates)
    val qcVariantIds = phaseSetsList.map(kv => kv._2)

    val gnocchiModelMetaData = buildMetaData(phenotypes, args.errorThreshold, args.associationType, args.covarNames, args.phenoName)

    // build GnocchiModel
    val gnocchiModel = args.associationType match {
      case "ADDITIVE_LINEAR" => {
        val variantModels = AdditiveLinearRegression(genotypeStates, phenotypes).map(_.toVariantModel)
        val comparisonModels = selectComparisonModels[AdditiveLinearVariantModel](variantModels, qcVariantIds)
        AdditiveLinearGnocchiModel(gnocchiModelMetaData, variantModels, comparisonModels)
      }
      case "DOMINANT_LINEAR" => {
        val variantModels = DominantLinearRegression(genotypeStates, phenotypes).map(_.toVariantModel)
        val comparisonModels = selectComparisonModels[DominantLinearVariantModel](variantModels, qcVariantIds)
        DominantLinearGnocchiModel(gnocchiModelMetaData, variantModels, comparisonModels)
      }
      case "ADDITIVE_LOGISTIC" => {
        val variantModels = AdditiveLogisticRegression(genotypeStates, phenotypes).map(_.toVariantModel)
        val comparisonModels = selectComparisonModels[AdditiveLogisticVariantModel](variantModels, qcVariantIds)
        AdditiveLogisticGnocchiModel(gnocchiModelMetaData, variantModels, comparisonModels)
      }
      case "DOMINANT_LOGISTIC" => {
        val variantModels = DominantLogisticRegression(genotypeStates, phenotypes).map(_.toVariantModel)
        val comparisonModels = selectComparisonModels[DominantLogisticVariantModel](variantModels, qcVariantIds)
        DominantLogisticGnocchiModel(gnocchiModelMetaData, variantModels, comparisonModels)
      }
    }

    // save the model
    gnocchiModel.save

  }

  def selectComparisonModels[VM <: VariantModel[VM]](variantModels: RDD[VM], variantsList: RDD[String]): RDD[(VM, Array[(Double, Array[Double])])] = {
    val qcVariantsList = variantsList.map(p => (p,p))
    variantModels.keyBy(vm => vm.variantId).join(qcVariantsList).map(kvv => kvv._)
  }

  def buildMetaData(phenotypes: RDD[Phenotype],
                    haplotypeBlockErrorThreshold: Double, modelType: String, variables: Option[String], phenotype: String): GnocchiModelMetaData = {
    // TODO: get the final numSamples after the Genotypes/Phenotypes join
    val numSamples = phenotypes.count.toInt
    new GnocchiModelMetaData(numSamples, haplotypeBlockErrorThreshold, modelType,
      // TODO: change GnocchiModelMetaData to use options rather than requiring a variables list
      // TODO: change "variables" field in GnocchiModelMetaData to "covariates".
      variables.getOrElse(""), List[String](), phenotype)
  }
}
