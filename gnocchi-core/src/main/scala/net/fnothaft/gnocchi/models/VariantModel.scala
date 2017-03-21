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
  protected def update(observations: Array[(Double, Array[Double])],
             locus: ReferenceRegion,
             altAllele: String,
             phenotype: String): VariantModel

  /**
   * Updates the weights for the model by averaging the previous weights with the
   * given weights. The batchWeights should be the result of a regression
   * run on the new batch.
   *
   * @param batchWeights The batch-optimized weights from a run of linearSiteRegression
   *                     on the site associated with the variantModel.
   * @return Returns updated weights
   */
  private def updateWeights(batchWeights: Array[Double]): Array[Double] = {

  }

  /**
   * Updates numSamples by adding number of samples in batch to the number
   * of samepls the model has already seen.
   *
   * @param batchNumSamples The number of samples in the new batch
   */
  def updateNumSamples(batchNumSamples: Int): Unit = {

  }

}

trait LinearVariantModel extends VariantModel {

  val variantId: String = "Empty Id"
  val ssDeviations = 0.0
  val ssResiduals = 0.0
  val geneticParameterStandardError = 0.0
  val tStatistic = 0.0
  val residualDegreesOfFreedom = 0
  val pValue = 0.0
  val variant = new Variant
  val weights = Array[Double]()
  val haplotypeBlock = "Nonexistent HaplotypeBlock"
  val numSamples = 0
  val association = Association(variant, "Empty Phenotype", 0.0, Map())

  /**
   * Updates the sum of squared deviations from the mean of the genotype at that site
   * by adding the sum of squared deviations from the batch to the sum of squared
   * deviations that the model already had.
   *
   * @note The mean used in the calculation is the batch mean, not the global mean.
   *
   * @param batchSsDeviations The sum of squared deviations of the genotype values
   *                              from the batch mean.
   */
  def updateSsDeviations(batchSsDeviations: Double): Unit = {

  }

  /**
   * Updates the sum of squared residuals for the model by adding the sum of squared
   * residuals for the batch to the sum of squared residuals that the model already
   * had.
   *
   * @note The estimated value for the phenotype is estimated based on the batch-
   *       optimized model, not the global model.
   *
   * @param batchSsResiduals The sum of squared residuals for the batch
   */
  def updateSsResiduals(batchSsResiduals: Double): Unit = {

  }

  /**
   * Updates the standard error of the genetic parameter based on current values
   * the VariantModel has in the ssResiduals, siteSSDeviations, and numSamples
   * parameters.
   */
  def updateGeneticParameterStandardError(): Unit = {

  }

  /**
   * Updates the geneticParameterDegreesOfFreedom for the model using the current
   * value for numSamples
   */
  def updateResidualDegreesOfFreedom(): Unit = {

  }

  /**
   * Updates the t statistic value for the VariantModel based on the current
   * values in weights, and geneticParameterStandard
   */
  def updateTStatistic(): Unit = {

  }

  /**
   * Updates the p-value for the VariantModel based on the current values in the
   * weights and residualDegreesOfFreedom parameters.
   */
  def updatePValue(): Unit = {

  }

  /**
   * Updates the VariantModel object's Association object with the current values
   * for weights, ssDeviations, ssResiduals, geneticParameterStandardError,
   * tstatistic, residualDegreesOfFreedom, and pValue
   */
  def updateAssociation(): Unit = {

  }

}

case class AdditiveLinearVariantModel extends LinearVariantModel with Serializable {

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
             phenotype: String): Unit = {
    val clippedObs = BuildAdditiveLinearVariantModel.arrayClipOrKeepState(observations)
    val assoc = AdditiveLinearAssociation.regressSite(clippedObs, variant, phenotype)
    /*
     * assumes that the relevant batch statistics are in the statistics dict in
     * the association object.
     */
    if (assoc.statistics.nonEmpty) {
      updateNumSamples(assoc.statistics("numSamples").asInstanceOf[Int])
      updateWeights(assoc.statistics("weights").asInstanceOf[Array[Double]])
      updateSsDeviations(assoc.statistics("ssDeviations").asInstanceOf[Double])
      updateSsResiduals(assoc.statistics("ssResiduals").asInstanceOf[Double])
      updateGeneticParameterStandardError()
      updateResidualDegreesOfFreedom()
      updateTStatistic()
      updatePValue()
      updateAssociation()
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
  def updateAssociation(): Association = {

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

//  val variantId = "Empty ID"
//  val variant = new Variant
//  val weights = Array[Double]()
//  val geneticParameterStandardError = 0.0
//  val pValue = 0.0
//  val haplotypeBlock = "Nonexistent HaplotypeBlock"
//  val numSamples = 0
//  val association = Association(variant, "Empty Phenotype", 0.0, Map())
//  val modelType = "Additive Logistic Variant Model"
//  val regressionName = "Additive Logistic Regression"

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
    if (assoc.statistics.nonEmpty) {
      updateNumSamples(assoc.statistics("numSamples").asInstanceOf[Int])
      updateGeneticParameterStandardError(assoc.statistics("standardError").asInstanceOf[Double])
      updateWeights(assoc.statistics("weights").asInstanceOf[Array[Double]])
      updateWaldStatistic()
      updatePvalue()
      updateAssociation()
    }
    AdditiveLogisticVariantModel(variantId, variant, updatedWeights,
      updatedGeneticParameterStandardErro, updatedPvalue, haplotypeBlock, updatedNumSamples,
      modelType)
  }
}
