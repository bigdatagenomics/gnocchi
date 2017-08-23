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
package net.fnothaft.gnocchi.models.linear

import java.io.{ File, FileOutputStream, ObjectOutputStream }

import net.fnothaft.gnocchi.algorithms.siteregression.DominantLinearRegression
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
import net.fnothaft.gnocchi.models.variant.linear.DominantLinearVariantModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bdgenomics.formats.avro.Variant

case class DominantLinearGnocchiModel(metaData: GnocchiModelMetaData,
                                      variantModels: RDD[DominantLinearVariantModel],
                                      comparisonVariantModels: RDD[(DominantLinearVariantModel, Array[(Double, Array[Double])])])
    extends GnocchiModel[DominantLinearVariantModel, DominantLinearGnocchiModel] {

  def constructGnocchiModel(metaData: GnocchiModelMetaData,
                            variantModels: RDD[DominantLinearVariantModel],
                            comparisonVariantModels: RDD[(DominantLinearVariantModel, Array[(Double, Array[Double])])]): DominantLinearGnocchiModel = {
    DominantLinearGnocchiModel(metaData, variantModels, comparisonVariantModels)
  }

  // calls the appropriate version of BuildVariantModel
  def regress(obs: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String,
              phaseSetId: Int): DominantLinearVariantModel = {
    DominantLinearRegression.applyToSite(obs, variant, metaData.phenotype, phaseSetId)
      .toVariantModel
  }

  def save(saveTo: String): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    import net.fnothaft.gnocchi.sql.AuxEncoders._
    sparkSession.createDataset(variantModels).toDF.write.parquet(saveTo + "/variantModels")
    sparkSession.createDataset(comparisonVariantModels.map(vmobs => {
      val (vm, observations) = vmobs
      new QualityControlVariantModel[DominantLinearVariantModel](vm, observations)
    }))
      .toDF.write.parquet(saveTo + "/qcModels")

    metaData.save(saveTo + "/metaData")
  }
}
