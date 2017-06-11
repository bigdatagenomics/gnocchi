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
package net.fnothaft.gnocchi.models.logistic

import java.io.{ File, FileOutputStream, ObjectOutputStream }

import net.fnothaft.gnocchi.algorithms.siteregression.DominantLogisticRegression
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.logistic.DominantLogisticVariantModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bdgenomics.formats.avro.Variant

case class DominantLogisticGnocchiModel(metaData: GnocchiModelMetaData,
                                        variantModels: RDD[DominantLogisticVariantModel],
                                        comparisonVariantModels: RDD[(DominantLogisticVariantModel, Array[(Double, Array[Double])])])
    extends GnocchiModel[DominantLogisticVariantModel, DominantLogisticGnocchiModel] {

  def constructGnocchiModel(metaData: GnocchiModelMetaData,
                            variantModels: RDD[DominantLogisticVariantModel],
                            comparisonVariantModels: RDD[(DominantLogisticVariantModel, Array[(Double, Array[Double])])]): DominantLogisticGnocchiModel = {
    DominantLogisticGnocchiModel(metaData, variantModels, comparisonVariantModels)
  }

  // calls the appropriate version of BuildVariantModel
  def regress(obs: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String,
              phaseSetId: Int): DominantLogisticVariantModel = {
    DominantLogisticRegression.applyToSite(obs, variant, metaData.phenotype, phaseSetId)
      .toVariantModel
  }

  def save(saveTo: String): Unit = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    variantModels.toDF.write.parquet(saveTo + "/variantModels")
    comparisonVariantModels.map(vmobs => {
      val (vm, observations) = vmobs
      new QualityControlVariant[DominantLogisticVariantModel](vm, observations)
    })
      .toDF.write.parquet(saveTo + "/qcModels")
    val metaDataFileStream = new FileOutputStream(new File(saveTo + "/metaData"))
    val metaDataObjectStream = new ObjectOutputStream(metaDataFileStream)
    metaDataObjectStream.writeObject(metaData)
  }
}
