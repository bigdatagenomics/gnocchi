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
package net.fnothaft.gnocchi.models.variant.logistic

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.primitives.association.LogisticAssociation
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.bdgenomics.formats.avro.Variant

trait LogisticVariantModel[VM <: LogisticVariantModel[VM]] extends VariantModel[VM] {
  val association: LogisticAssociation

  /**
   * Returns updated LogisticVariantModel of correct subtype
   *
   * @param variantModel Variant model whose parameters are to be used to update
   *                     existing variant model.
   *
   * @return Returns updated LogisticVariantModel of correct subtype
   */
  def mergeWith(variantModel: VM): VM = {
    val updatedNumSamples = updateNumSamples(variantModel.association.numSamples)
    val updatedGeneticParameterStandardError = computeGeneticParameterStandardError(variantModel.association.geneticParameterStandardError, variantModel.association.numSamples)
    val updatedWeights = updateWeights(variantModel.association.weights, variantModel.association.numSamples)
    val updatedWaldStatistic = calculateWaldStatistic(updatedGeneticParameterStandardError, updatedWeights)
    val updatedPValue = calculatePvalue(updatedWaldStatistic)
    constructVariantModel(this.uniqueID,
      updatedGeneticParameterStandardError,
      updatedPValue,
      updatedWeights,
      updatedNumSamples)
  }

  /**
   * Updates the standard error of the genetic parameter by averaging batch's standard
   * error of the genetic parameter with the model's current standard error of the
   * genetic parameter.
   *
   * @note Averaging standard errors because unlike in linear regression,
   *       there is no obvious way to combine intermediate values
   * @param batchStandardError Standard error of the genetic parameter in the batch of
   *                           data.
   * @param batchNumSamples Number of samples in the update batch
   */
  def computeGeneticParameterStandardError(batchStandardError: Double, batchNumSamples: Int): Double = {
    (batchStandardError * batchNumSamples.toDouble + association.geneticParameterStandardError * association.numSamples.toDouble) / (batchNumSamples + association.numSamples).toDouble

  }

  /**
   * Returns the wald statistic, calculated by taking the square of the ratio
   * of the weight associated with the genetic parameter and the standard error of the
   * genetic component.
   *
   * @param geneticParameterStandardError Standard error of the genetic parameter
   * @param weights model weights
   * @return Returns wald statistic for genetic parameter
   */
  def calculateWaldStatistic(geneticParameterStandardError: Double, weights: List[Double]): Double = {
    math.pow(weights(1) / geneticParameterStandardError, 2)
  }

  /**
   * Returns  the p value associated with the genetic parameter in the regression model
   * by running the wald statistic associated with genetic parameter through chi distribution
   * with one degree of freedom.
   *
   * @param waldStatistic Wald statistic to be used in p value calculation
   * @return
   */
  def calculatePvalue(waldStatistic: Double): Double = {
    val chiDist = new ChiSquaredDistribution(1)
    1 - chiDist.cumulativeProbability(waldStatistic)
  }

  def constructVariantModel(variantId: String,
                            updatedGeneticParameterStandardError: Double,
                            updatedPValue: Double,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): VM
}
