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
package net.fnothaft.gnocchi.models

import net.fnothaft.gnocchi.association.{ AdditiveLinearAssociation, AdditiveLogisticAssociation }
import net.fnothaft.gnocchi.gnocchiModel.{ BuildAdditiveLinearVariantModel, BuildAdditiveLogisticVariantModel }
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

trait LinearVariantModel extends VariantModel {

  val variantId: String
  val ssDeviations: Double
  val ssResiduals: Double
  val geneticParameterStandardError: Double
  val tStatistic: Double
  val residualDegreesOfFreedom: Int
  val pValue: Double
  val variant: Variant
  val weights: Array[Double]
  val haplotypeBlock: String
  val numSamples: Int
  val association: Association

  /**
   * Returns updated the sum of squared deviations from the mean of the genotype at that site
   * by adding the sum of squared deviations from the batch to the sum of squared
   * deviations that the model already had.
   *
   * @note The mean used in the calculation is the batch mean, not the global mean.
   *
   * @param batchSsDeviations The sum of squared deviations of the genotype values
   *                              from the batch mean.
   */
  protected def updateSsDeviations(batchSsDeviations: Double): Double = {

  }

  /**
   * Returns updated sum of squared residuals for the model by adding the sum of squared
   * residuals for the batch to the sum of squared residuals that the model already
   * had.
   *
   * @note The estimated value for the phenotype is estimated based on the batch-
   *       optimized model, not the global model.
   *
   * @param batchSsResiduals The sum of squared residuals for the batch
   */
  protected def updateSsResiduals(batchSsResiduals: Double): Double = {

  }

  /**
   * Returns updated standard error of the genetic parameter based on current values
   * the VariantModel has in the ssResiduals, ssDeviations, and numSamples
   * parameters.
   */
  protected def updateGeneticParameterStandardError(updatedSsResiduals: Double,
                                                  updatedSsDeviations: Double,
                                                  updatedNumSamples: Int): Double = {

  }

  /**
   * Returns updated geneticParameterDegreesOfFreedom for the model using the current
   * value for numSamples
   */
  protected def updateResidualDegreesOfFreedom(batchNumSamples: Int): Int = {

  }

  /**
   * Returns updated t statistic value for the VariantModel based on the current
   * values in weights, and geneticParameterStandardError.
   */
  protected def updateTStatistic(updatedWeights: Array[Double],
                               updatedGeneticParameterStandardError:Double): Double = {

  }

  /**
   * Returns updated p-value for the VariantModel based on the current values in the
   * tStatistic and residualDegreesOfFreedom parameters.
   */
  protected def updatePValue(updatedTStatistic: Double,
                           updatedResidualDegreesOfFreedom: Int): Double = {

  }

  /**
   * Returns new Association object with provided values for
   * for weights, ssDeviations, ssResiduals, geneticParameterStandardError,
   * tstatistic, residualDegreesOfFreedom, and pValue, and current
    * VariantModel's Association object.
   */
  protected def updateAssociation(updatedWeights: Array[Double],
                                  updatedSsDeviations: Double,
                                  updatedSsResiduals: Double,
                                  updatedGeneticParameterStandardError: Double,
                                  updatedTStatistic: Double,
                                  updatedResidualDegreesOfFreedom: Int,
                                  updatedPValue: Double): Association = {

  }

}

case class AdditiveLinearVariantModel(variantId: String,
                                      ssDeviations: Double,
                                      ssResiduals: Double,
                                      geneticParameterStandardError: Double,
                                      tStatistic: Double,
                                      residualDegreesOfFreedom: Int,
                                      pValue: Double,
                                      variant: Variant,
                                      weights: Array[Double],
                                      haplotypeBlock: String,
                                      numSamples: Int,
                                      association: Association) extends LinearVariantModel with Serializable {

  val modelType = "Additive Linear Variant Model"
  val regressionName = "Additive Linear Regression"

  /**
   * Updates the VariantModel given a new batch of data
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
  def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): VariantModel = {
    val clippedObs = BuildAdditiveLinearVariantModel.arrayClipOrKeepState(observations)
    val assoc = AdditiveLinearAssociation.regressSite(clippedObs, variant, phenotype)
    /*
     * assumes that the relevant batch statistics are in the statistics dict in
     * the association object.
     */
    val updatedNumSamples = updateNumSamples(assoc.statistics("numSamples").asInstanceOf[Int])
    val updatedWeights = updateWeights(assoc.statistics("weights").asInstanceOf[Array[Double]])
    val updatedSsDeviations = updateSsDeviations(assoc.statistics("ssDeviations").asInstanceOf[Double])
    val updatedSsResiduals = updateSsResiduals(assoc.statistics("ssResiduals").asInstanceOf[Double])
    val updatedGeneticParameterStandardError = updateGeneticParameterStandardError(updatedSsResiduals,
      updatedSsDeviations, updatedNumSamples)
    val updatedResidualDegreesOfFreedom = updateResidualDegreesOfFreedom(assoc.statistics("numSamples").asInstanceOf[Int])
    val updatedtStatistic = updateTStatistic(updatedWeights, updatedGeneticParameterStandardError)
    val updatedPValue = updatePValue(updatedtStatistic, updatedResidualDegreesOfFreedom)
    val updatedAssociation = updateAssociation(updatedWeights, updatedSsDeviations, updatedSsResiduals,
      updatedGeneticParameterStandardError, updatedtStatistic, updatedResidualDegreesOfFreedom, updatedPValue)

    if (assoc.statistics.nonEmpty) {
      AdditiveLinearVariantModel(this.variantId,
                                     updatedSsDeviations,
                                     updatedSsResiduals,
                                     updatedGeneticParameterStandardError,
                                     updatedtStatistic,
                                     updatedResidualDegreesOfFreedom,
                                     updatedPValue,
                                     this.variant,
                                     updatedWeights,
                                     this.haplotypeBlock,
                                     updatedNumSamples,
                                     updatedAssociation)
    }
  }
}

trait LogisticVariantModel extends VariantModel {

  /**
   * Updates the standard error of the genetic parameter by averaging batch's standard
   * error of the genetic parameter with the model's current standard error of the
   * genetic parameter.
   *
   * @note Averaging standard errors because unlike in linear regression,
   *       there is no obvious way to combine intermediate values
   * @param batchStandardError Standard error of the genetic parameter in the batch of
   *                           data.
   */
  def updateGeneticParameterStandardError(batchStandardError: Double): Double = {

  }

  /**
   * Updates the Wald statistic (waldStatistic) for the genetic parameter based on the current
   * values for the GeneticParameterStandardError and weights.
   */
  def updateWaldStatistic(GeneticParameterStandardError: Double, weights: Array[Double]): Double = {

  }

  /**
   * Updates the pValue associated with the VariantModel
   */
  def updatePvalue(waldStatistic: Double): Double = {

  }

  /**
   * Updates the Association object associated with the VariantModel
   */
  def updateAssociation(geneticParameterStandardError: Double,
                        pValue: Double,
                        numSamples: Int,
                        weights: Array[Double]): Association = {

  }

}

case class AdditiveLogisticVariantModel(variantId: String,
                                        variant: Variant,
                                        weights: Array[Double],
                                        geneticParameterStandardError: Double,
                                        pValue: Double,
                                        haplotypeBlock: String,
                                        numSamples: Int,
                                        modelType: String) extends LogisticVariantModel with Serializable {


  val modelType = "Additive Logistic Variant Model"
  val regressionName = "Additive Logistic Regression"

  /**
   * Updates the VariantModel given a new batch of data
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
  def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): AdditiveLogisticVariantModel = {

    val clippedObs = BuildAdditiveLogisticVariantModel.arrayClipOrKeepState(observations)
    val assoc = AdditiveLogisticAssociation.regressSite(clippedObs, variant, phenotype)

    val updatedNumSamples = updateNumSamples(assoc.statistics("numSamples").asInstanceOf[Int])
    val updatedGeneticParameterStandardError = updateGeneticParameterStandardError(assoc.statistics("standardError").asInstanceOf[Double])
    val updatedWeights = updateWeights(assoc.statistics("weights").asInstanceOf[Array[Double]])
    val updatedWaldStatistic = updateWaldStatistic(updatedGeneticParameterStandardError, updatedWeights)
    val updatedPValue = updatePvalue(updatedWaldStatistic)
    val updatedAssociation = updateAssociation(updatedGeneticParameterStandardError, updatedPValue, updatedNumSamples, updatedWeights)
    if (assoc.statistics.nonEmpty) {
      AdditiveLogisticVariantModel(this.variantId,
                                   this.variant,
                                   updatedWeights,
                                   updatedGeneticParameterStandardError,
                                   updatedPValue,
                                   this.haplotypeBlock,
                                   updatedNumSamples,
                                   this.modelType )
    }
  }
}
