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

package org.bdgenomics.gnocchi.models.variant.logistic

import org.bdgenomics.gnocchi.algorithms.siteregression.DominantLogisticRegression
import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.commons.math3.linear.SingularMatrixException

import scala.collection.immutable.Map

case class DominantLogisticVariantModel(uniqueID: String,
                                        association: LogisticAssociation,
                                        phenotype: String,
                                        chromosome: Int,
                                        position: Int,
                                        referenceAllele: String,
                                        alternateAllele: String,
                                        phaseSetId: Int = 0)
    extends LogisticVariantModel[DominantLogisticVariantModel]
    with DominantLogisticRegression with Serializable {

  val modelType = "Dominant Logistic Variant Model"
  override val regressionName = "Dominant Logistic Regression"

  /**
   * Updates the LogisticVariantModel given a new batch of data
   *
   * @param observations Array containing data at the particular site for
   *                     all samples. Format of each element is:
   *                     (gs, Array(pheno, covar1, ... covarp))
   *                     where gs is the diploid genotype at that site for the
   *                     given sample [0, 1, or 2], pheno is the sample's value for
   *                     the primary phenotype being regressed on, and covar1-covarp
   *                     are that sample's values for each covariate.
   */
  def update(genotypes: CalledVariant, phenotypes: Map[String, Phenotype]): DominantLogisticVariantModel = {

    //TODO: add validation stringency here rather than just creating empty association object
    val batchVariantModel = try {
      constructVariantModel(uniqueID, applyToSite(phenotypes, genotypes))
    } catch {
      case error: SingularMatrixException => throw new SingularMatrixException()
    }
    mergeWith(batchVariantModel)
  }

  def constructVariantModel(variantId: String,
                            updatedGeneticParameterStandardError: Double,
                            updatedPValue: Double,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): DominantLogisticVariantModel = {

    val association = LogisticAssociation(weights = updatedWeights,
      geneticParameterStandardError = updatedGeneticParameterStandardError,
      pValue = updatedPValue,
      numSamples = updatedNumSamples)

    DominantLogisticVariantModel(variantId,
      association,
      phenotype,
      chromosome,
      position,
      referenceAllele,
      alternateAllele,
      phaseSetId)
  }

  def constructVariantModel(variantId: String,
                            association: LogisticAssociation): DominantLogisticVariantModel = {

    DominantLogisticVariantModel(variantId,
      association,
      phenotype,
      chromosome,
      position,
      referenceAllele,
      alternateAllele,
      phaseSetId)
  }
}
