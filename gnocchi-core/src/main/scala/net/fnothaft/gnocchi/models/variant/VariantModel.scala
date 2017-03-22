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

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLogisticAssociation
import net.fnothaft.gnocchi.gnocchiModel.{BuildAdditiveLinearVariantModel, BuildAdditiveLogisticVariantModel}
import net.fnothaft.gnocchi.rdd.association.Association
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

trait VariantModel extends Serializable {
  val variantId: String
  val variant: Variant
  val numSamples: Int
  val modelType: String // e.g. Additive Logistic, Dominant Linear, etc.
  val weights: Array[Double]
  val pValue: Double
  val geneticParameterStandardError: Double
  val haplotypeBlock: String
  val association: Association

  /**
   * Returns an updated VariantModel given a new batch of data
   *
   * @note observations is an array of tuples with (genotypeState, array of phenotypes)
   *       where the array of phenotypes has the primary phenotype as the first
   *       value and covariates following it.
   * @param observations Array containing data at the particular site for
   *                     all samples. Format of each element is:
   *                     (gs, Array(pheno, covar1, ... covarp))
   *                     where gs is the diploid genotype at that site for the
   *                     given sample [0, 1, or 2], pheno is the sample's value for
   *                     the phenotype being regressed on, and covar1-covarp are that
   *                     sample's values for each covariate.
   * @param locus Locus of the variant
   * @param altAllele Alternate allele
   * @param phenotype Text description of the phenotype and covariates being
   *                  considered.
   */
  protected def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): VariantModel

  /**
   * Returns updated weights for the model by averaging the previous weights with the
   * given weights. The batchWeights should be the result of a regression
   * run on the new batch.
   *
   * @param batchWeights The batch-optimized weights from a run of linearSiteRegression
   *                     on the site associated with the variantModel.
   * @return Returns updated weights
   */
  def updateWeights(batchWeights: Array[Double]): Array[Double] = {

  }

  /**
   * Returns updated numSamples by adding number of samples in batch to the number
   * of samepls the model has already seen.
   *
   * @param batchNumSamples The number of samples in the new batch
   */
  def updateNumSamples(batchNumSamples: Int): Int = {

  }

}








