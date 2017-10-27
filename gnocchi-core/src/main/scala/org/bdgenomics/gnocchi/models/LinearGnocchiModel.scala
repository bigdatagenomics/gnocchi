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
package org.bdgenomics.gnocchi.models

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
import org.bdgenomics.gnocchi.models.variant.{ LinearVariantModel, QualityControlVariantModel }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

object LinearGnocchiModelFactory {

  val regressionName = "LinearRegression"

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: Broadcast[Map[String, Phenotype]],
            phenotypeNames: Option[List[String]],
            QCVariantIDs: Option[Set[String]] = None,
            QCVariantSamplingRate: Double = 0.1,
            allelicAssumption: String = "ADDITIVE",
            validationStringency: String = "STRICT"): LinearGnocchiModel = {

    import genotypes.sqlContext.implicits._

    // ToDo: sampling QC Variants better.
    val variantModels = LinearSiteRegression(genotypes, phenotypes, allelicAssumption = allelicAssumption, validationStringency = validationStringency)

    // Create QCVariantModels
    val comparisonVariants = if (QCVariantIDs.isEmpty) {
      genotypes.sample(withReplacement = false, fraction = 0.1)
    } else {
      genotypes.filter(x => QCVariantIDs.get.contains(x.uniqueID))
    }

    val QCVariantModels = variantModels
      .joinWith(comparisonVariants, variantModels("uniqueID") === comparisonVariants("uniqueID"), "inner")
      .withColumnRenamed("_1", "variantModel")
      .withColumnRenamed("_2", "variant")
      .as[QualityControlVariantModel[LinearVariantModel]]

    val phenoNames = if (phenotypeNames.isEmpty) {
      List(phenotypes.value.head._2.phenoName) ++ (1 to phenotypes.value.head._2.covariates.length).map(x => "covar_" + x)
    } else {
      phenotypeNames.get
    }

    // Create metadata
    val metadata = GnocchiModelMetaData(regressionName,
      phenoNames.head,
      phenoNames.tail.mkString(","),
      genotypes.count().toInt,
      flaggedVariantModels = Option(QCVariantModels.select("variant.uniqueID").as[String].collect().toList))

    LinearGnocchiModel(metaData = metadata,
      variantModels = variantModels,
      QCVariantModels = QCVariantModels,
      QCPhenotypes = phenotypes.value)
  }
}

case class LinearGnocchiModel(metaData: GnocchiModelMetaData,
                              variantModels: Dataset[LinearVariantModel],
                              QCVariantModels: Dataset[QualityControlVariantModel[LinearVariantModel]],
                              QCPhenotypes: Map[String, Phenotype])
    extends GnocchiModel[LinearVariantModel, LinearGnocchiModel] {

  import variantModels.sqlContext.implicits._

  def mergeGnocchiModel(otherModel: GnocchiModel[LinearVariantModel, LinearGnocchiModel]): GnocchiModel[LinearVariantModel, LinearGnocchiModel] = {

    require(otherModel.metaData.modelType == metaData.modelType,
      "Models being merged are not the same type. Type equality is required to merge two models correctly.")

    val mergedVMs = mergeVariantModels(otherModel.getVariantModels)

    // ToDo: 1. [DONE] make sure models are of same type 2. [DONE] find intersection of QCVariants and use those as the gnocchiModel
    // ToDo: QCVariants 3. Make sure the phenotype of the models are the same 4. Make sure the covariates of the model
    // ToDo: are the same (currently broken because covariates stored in [[Phenotype]] object are the values not names)
    val updatedMetaData = updateMetaData(otherModel.metaData.numSamples)

    val mergedQCVariants = mergeQCVariants(otherModel.QCVariantModels)
    val mergedQCVariantModels = mergedVMs.joinWith(mergedQCVariants, mergedVMs("uniqueID") === mergedQCVariants("uniqueID"), "inner")
      .withColumnRenamed("_1", "variantModel")
      .withColumnRenamed("_2", "variant")
      .as[QualityControlVariantModel[LinearVariantModel]]
    val mergedQCPhenotypes = QCPhenotypes ++ otherModel.QCPhenotypes

    LinearGnocchiModel(updatedMetaData, mergedVMs, mergedQCVariantModels, mergedQCPhenotypes)
  }

  def mergeVariantModels(newVariantModels: Dataset[LinearVariantModel]): Dataset[LinearVariantModel] = {
    variantModels.joinWith(newVariantModels, variantModels("uniqueID") === newVariantModels("uniqueID")).map(x => x._1.mergeWith(x._2))
  }

  def mergeQCVariants(newQCVariantModels: Dataset[QualityControlVariantModel[LinearVariantModel]]): Dataset[CalledVariant] = {
    val variants1 = QCVariantModels.map(_.variant)
    val variants2 = newQCVariantModels.map(_.variant)

    variants1.joinWith(variants2, variants1("uniqueID") === variants2("uniqueID"))
      .as[(CalledVariant, CalledVariant)]
      .map(x =>
        CalledVariant(x._1.chromosome,
          x._1.position,
          x._1.uniqueID,
          x._1.referenceAllele,
          x._1.alternateAllele,
          x._1.qualityScore,
          x._1.filter,
          x._1.info,
          x._1.format,
          x._1.samples ++ x._2.samples))
  }
}
