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
package org.bdgenomics.gnocchi.models.variant

import org.bdgenomics.gnocchi.primitives.association.Association
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant

import scala.collection.immutable.Map

trait VariantModel[VM <: VariantModel[VM]] {
  val uniqueID: String
  val modelType: String
  val phenotype: String
  val chromosome: Int
  val position: Int
  val referenceAllele: String
  val alternateAllele: String
  val association: Association

  /**
   * Returns an updated VariantModel given a new batch of data
   *
   * @param observations Array containing data at the particular site for
   *                     all samples. Format of each element is:
   *                     (gs, Array(pheno, covar1, ... covarp))
   *                     where gs is the diploid genotype at that site for the
   *                     given sample [0, 1, or 2], pheno is the sample's value for
   *                     the phenotype being regressed on, and covar1-covarp are that
   *                     sample's values for each covariate.
   */
  def update(genotypes: CalledVariant, phenotypes: Map[String, Phenotype]): VM

  /**
   * Returns updated weights for the model taking a weighted average of the previous
   * weights with the given weights and number of samples. The batchWeights should
   * be the result of a regression run on the new batch of data.
   *
   * @param batchWeights The batch-optimized weights from a run of linearSiteRegression
   *                     or logisticSiteRegression on the site associated with the
   *                     VariantModel.
   * @param batchNumSamples Number of samples in the new batch of data
   * @return Returns updated weights
   */
  def updateWeights(batchWeights: List[Double], batchNumSamples: Int): List[Double] = {
    val updatedWeights = new Array[Double](association.weights.length)
    for (i <- association.weights.indices) {
      updatedWeights(i) = (batchWeights(i) * batchNumSamples + association.weights(i) * association.numSamples) / (batchNumSamples + association.numSamples)
    }
    updatedWeights.toList
  }

  /**
   * Returns updated numSamples by adding number of samples in batch to the number
   * of samples the model has already seen.
   *
   * @param batchNumSamples The number of samples in the new batch
   * @return Returns updated number of samples
   */
  def updateNumSamples(batchNumSamples: Int): Int = {
    association.numSamples + batchNumSamples
  }
}

