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

import java.io.{ File, FileOutputStream, ObjectOutputStream, PrintWriter }

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearRegression
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bdgenomics.formats.avro.Variant
import scala.pickling.Defaults._
import scala.pickling.json._

case class AdditiveLinearGnocchiModel(metaData: GnocchiModelMetaData,
                                      variantModels: RDD[AdditiveLinearVariantModel],
                                      comparisonVariantModels: RDD[(AdditiveLinearVariantModel, Array[(Double, Array[Double])])])
    extends GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel] {

  def constructGnocchiModel(metaData: GnocchiModelMetaData,
                            variantModels: RDD[AdditiveLinearVariantModel],
                            comparisonVariantModels: RDD[(AdditiveLinearVariantModel, Array[(Double, Array[Double])])]): AdditiveLinearGnocchiModel = {
    AdditiveLinearGnocchiModel(metaData, variantModels, comparisonVariantModels)
  }

  def regress(obs: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String,
              phaseSetId: Int): AdditiveLinearVariantModel = {
    AdditiveLinearRegression.applyToSite(obs, variant, metaData.phenotype, phaseSetId).toVariantModel
  }

  def save(saveTo: String): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    import net.fnothaft.gnocchi.sql.AuxEncoders._
    sparkSession.createDataset(variantModels).toDF.write.parquet(saveTo + "/variantModels")
    sparkSession.createDataset(comparisonVariantModels.map(vmobs => {
      val (vm, observations) = vmobs
      new QualityControlVariantModel[AdditiveLinearVariantModel](vm, observations)
    }))
      .toDF.write.parquet(saveTo + "/qcModels")

    metaData.save(saveTo + "/metaData")
  }
}
