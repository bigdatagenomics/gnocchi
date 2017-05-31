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
package net.fnothaft.gnocchi.models.variant.linear

import net.fnothaft.gnocchi.algorithms.siteregression.DominantLinearRegression
import org.bdgenomics.formats.avro.Variant

case class DominantLinearVariantModel(variantId: String,
                                      ssDeviations: Double,
                                      ssResiduals: Double,
                                      geneticParameterStandardError: Double,
                                      tStatistic: Double,
                                      residualDegreesOfFreedom: Int,
                                      pValue: Double,
                                      variant: Variant,
                                      weights: List[Double],
                                      numSamples: Int,
                                      phenotype: String,
                                      phaseSetId: Int = 0)
    extends LinearVariantModel[DominantLinearVariantModel]
    with DominantLinearRegression with Serializable {

  type VM = DominantLinearVariantModel
  val modelType = "Dominant Linear Variant Model"
  val regressionName = "Dominant Linear Regression"

  /**
   * Updates the DominantLinearVariantModel given a new batch of data
   *
   * @param observations Array containing data at the particular site for
   *                     all samples. Format of each element is:
   *                     (gs, Array(pheno, covar1, ... covarp))
   *                     where gs is the diploid genotype at that site for the
   *                     given sample [0, 1, or 2], pheno is the sample's value for
   *                     the primary phenotype being regressed on, and covar1-covarp
   *                     are that sample's values for each covariate.
   */
  def update(observations: Array[(Double, Array[Double])]): DominantLinearVariantModel = {
    val batchVariantModel = applyToSite(observations, variant, phenotype, phaseSetId)
      .toVariantModel
    mergeWith(batchVariantModel)
  }

  def constructVariantModel(variantID: String,
                            updatedSsDeviations: Double,
                            updatedSsResiduals: Double,
                            updatedGeneticParameterStandardError: Double,
                            updatedtStatistic: Double,
                            updatedResidualDegreesOfFreedom: Int,
                            updatedPValue: Double,
                            variant: Variant,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): DominantLinearVariantModel = {
    DominantLinearVariantModel(variantID,
      updatedSsDeviations,
      updatedSsResiduals,
      updatedGeneticParameterStandardError,
      updatedtStatistic,
      updatedResidualDegreesOfFreedom,
      updatedPValue,
      variant,
      updatedWeights,
      updatedNumSamples,
      phenotype,
      phaseSetId)
  }

}
