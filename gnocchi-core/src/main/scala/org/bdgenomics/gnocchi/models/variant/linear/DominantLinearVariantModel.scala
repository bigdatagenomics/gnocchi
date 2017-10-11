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
package org.bdgenomics.gnocchi.models.variant.linear

import org.bdgenomics.gnocchi.algorithms.siteregression.DominantLinearRegression
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

case class DominantLinearVariantModel(uniqueID: String,
                                      association: LinearAssociation,
                                      phenotype: String,
                                      chromosome: Int,
                                      position: Int,
                                      referenceAllele: String,
                                      alternateAllele: String,
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
  def update(genotypes: CalledVariant, phenotypes: Map[String, Phenotype]): DominantLinearVariantModel = {
    val batchVariantModel = constructVariantModel(uniqueID, applyToSite(phenotypes, genotypes))
    mergeWith(batchVariantModel)
  }

  def constructVariantModel(variantID: String,
                            updatedSsDeviations: Double,
                            updatedSsResiduals: Double,
                            updatedGeneticParameterStandardError: Double,
                            updatedtStatistic: Double,
                            updatedResidualDegreesOfFreedom: Int,
                            updatedPValue: Double,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): DominantLinearVariantModel = {

    val updatedAssociation = LinearAssociation(ssDeviations = updatedSsDeviations,
      ssResiduals = updatedSsResiduals,
      geneticParameterStandardError = updatedGeneticParameterStandardError,
      tStatistic = updatedtStatistic,
      residualDegreesOfFreedom = updatedResidualDegreesOfFreedom,
      pValue = updatedPValue,
      weights = updatedWeights,
      numSamples = updatedNumSamples)

    DominantLinearVariantModel(variantID,
      updatedAssociation,
      phenotype,
      chromosome,
      position,
      referenceAllele,
      alternateAllele,
      phaseSetId)
  }

  def constructVariantModel(variantID: String,
                            association: LinearAssociation): DominantLinearVariantModel = {
    DominantLinearVariantModel(variantID,
      association,
      phenotype,
      chromosome,
      position,
      referenceAllele,
      alternateAllele, phaseSetId)
  }

}
