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
package net.fnothaft.gnocchi.models.variant

import org.bdgenomics.formats.avro.Variant
import org.apache.spark.SparkContext._

trait VariantModel[VM <: VariantModel[VM]] {
  val variantId: String
  val variant: Variant
  val numSamples: Int
  val modelType: String // e.g. Additive Logistic, Dominant Linear, etc.
  val weights: List[Double] // using a List so that comparison of VariantModels can be done with ==
  val pValue: Double
  val geneticParameterStandardError: Double
  val phenotype: String
  val phaseSetId: Int

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
  def update(observations: Array[(Double, Array[Double])]): VM

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
    val updatedWeights = new Array[Double](weights.length)
    for (i <- weights.indices) {
      updatedWeights(i) = (batchWeights(i) * batchNumSamples + weights(i) * numSamples) / (batchNumSamples + numSamples)
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
    numSamples + batchNumSamples
  }

}

