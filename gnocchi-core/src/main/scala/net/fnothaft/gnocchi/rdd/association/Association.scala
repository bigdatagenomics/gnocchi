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
package net.fnothaft.gnocchi.rdd.association

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import org.bdgenomics.formats.avro.Variant

trait Association[VM <: VariantModel[VM]] {
  implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[Association[VM]]
  val variantId: String
  val numSamples: Int
  val modelType: String
  val weights: Array[Double]
  val geneticParameterStandardError: Double
  val variant: Variant
  val phenotype: String
  val logPValue: Double
  val pValue: Double
  val statistics: Map[String, Any]

  def toVariantModel: VM
}

case class AdditiveLinearAssociation(variantId: String,
                                     numSamples: Int,
                                     modelType: String,
                                     weights: Array[Double],
                                     geneticParameterStandardError: Double,
                                     variant: Variant,
                                     phenotype: String,
                                     logPValue: Double,
                                     pValue: Double,
                                     phaseSetId: Int,
                                     statistics: Map[String, Any]) extends Association[AdditiveLinearVariantModel] {
  /**
   * Converts Association object into a VariantModel object of the
   * appropriate subtype.
   *
   * @return Returns a VariantModel object of the appropriate subtype.
   */
  def toVariantModel: AdditiveLinearVariantModel = {
    assert(statistics.nonEmpty, "Cannot convert association object with empty statistics map to linear VariantModel")
    val ssDeviations = statistics.get("ssDeviations").asInstanceOf[Option[Double]]
    val ssResiduals = statistics.get("ssResiduals").asInstanceOf[Option[Double]]
    val tStatistic = statistics.get("tStatistic").asInstanceOf[Option[Double]]
    val residualDegreesOfFreedom = statistics.get("residualDegreesOfFreedom").asInstanceOf[Option[Int]]
    assert(ssDeviations.isDefined, "'ssDeviations' must be defined in statistics map" +
      "in AdditiveLinearAssociation object for the object to be converted to AdditiveLinearVariantModel")
    assert(ssResiduals.isDefined, "'ssResiduals' must be defined in statistics map" +
      "in AdditiveLinearAssociation object for the object to be converted to AdditiveLinearVariantModel")
    assert(tStatistic.isDefined, "'tStatistic' must be defined in statistics map" +
      "in AdditiveLinearAssociation object for the object to be converted to AdditiveLinearVariantModel")
    assert(residualDegreesOfFreedom.isDefined, "'residualDegreesOfFreedom' must be defined in statistics map" +
      "in AdditiveLinearAssociation object for the object to be converted to AdditiveLinearVariantModel")
    AdditiveLinearVariantModel(variantId,
      ssDeviations.get, ssResiduals.get, geneticParameterStandardError.toDouble,
      tStatistic.get, residualDegreesOfFreedom.get, pValue, variant,
      weights.toList, numSamples, phenotype, phaseSetId)
  }
}

case class AdditiveLogisticAssociation(variantId: String,
                                       numSamples: Int,
                                       modelType: String,
                                       weights: Array[Double],
                                       geneticParameterStandardError: Double,
                                       variant: Variant,
                                       phenotype: String,
                                       logPValue: Double,
                                       pValue: Double,
                                       phaseSetId: Int,
                                       statistics: Map[String, Any]) extends Association[AdditiveLogisticVariantModel] {
  /**
   * Converts Association object into a VariantModel object of the
   * appropriate subtype.
   *
   * @return Returns a VariantModel object of the appropriate subtype.
   */
  def toVariantModel: AdditiveLogisticVariantModel = {
    println("Weights in association")
    println(weights.toList)
    AdditiveLogisticVariantModel(variantId,
      variant, weights.toList, geneticParameterStandardError.toDouble, pValue,
      numSamples, phenotype, phaseSetId)
  }
}

case class DominantLinearAssociation(variantId: String,
                                     numSamples: Int,
                                     modelType: String,
                                     weights: Array[Double],
                                     geneticParameterStandardError: Double,
                                     variant: Variant,
                                     phenotype: String,
                                     logPValue: Double,
                                     pValue: Double,
                                     phaseSetId: Int,
                                     statistics: Map[String, Any]) extends Association[DominantLinearVariantModel] {
  /**
   * Converts Association object into a VariantModel object of the
   * appropriate subtype.
   *
   * @return Returns a VariantModel object of the appropriate subtype.
   */
  def toVariantModel: DominantLinearVariantModel = {
    assert(statistics.nonEmpty, "Cannot convert association object with empty statistics map to VariantModel")
    val ssDeviations = statistics("ssDeviations").asInstanceOf[Double]
    val ssResiduals = statistics("ssResiduals").asInstanceOf[Double]
    val tStatistic = statistics("tStatistic").asInstanceOf[Double]
    val residualDegreesOfFreedom = statistics("residualDegreesOfFreedom").asInstanceOf[Int]
    DominantLinearVariantModel(variantId,
      ssDeviations, ssResiduals, geneticParameterStandardError,
      tStatistic, residualDegreesOfFreedom, pValue, variant,
      weights.toList, numSamples, phenotype, phaseSetId)
  }
}

case class DominantLogisticAssociation(variantId: String,
                                       numSamples: Int,
                                       modelType: String,
                                       weights: Array[Double],
                                       geneticParameterStandardError: Double,
                                       variant: Variant,
                                       phenotype: String,
                                       logPValue: Double,
                                       pValue: Double,
                                       phaseSetId: Int,
                                       statistics: Map[String, Any]) extends Association[DominantLogisticVariantModel] {
  /**
   * Converts Association object into a VariantModel object of the
   * appropriate subtype.
   *
   * @return Returns a VariantModel object of the appropriate subtype.
   */
  def toVariantModel: DominantLogisticVariantModel = {
    DominantLogisticVariantModel(variantId,
      variant, weights.toList, geneticParameterStandardError, pValue,
      numSamples, phenotype, phaseSetId)
  }
}

