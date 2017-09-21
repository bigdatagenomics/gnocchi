package net.fnothaft.gnocchi.models.variant.linear

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.primitives.association.LinearAssociation
import org.apache.commons.math3.distribution.TDistribution
import org.bdgenomics.formats.avro.Variant

trait LinearVariantModel[VM <: LinearVariantModel[VM]] extends VariantModel[VM] {
  val association: LinearAssociation

  /**
   * Returns updated LinearVariantModel of correct subtype
   *
   * @param variantModel Variant model whose parameters are to be used to update
   *                     existing variant model.
   *
   * @return Returns updated LinearVariantModel of correct subtype
   */
  def mergeWith(variantModel: VM): VM = {
    val updatedNumSamples = updateNumSamples(variantModel.association.numSamples)
    val updatedWeights = updateWeights(variantModel.association.weights, variantModel.association.numSamples)
    val updatedSsDeviations = updateSsDeviations(variantModel.association.ssDeviations)
    val updatedSsResiduals = updateSsResiduals(variantModel.association.ssResiduals)
    val updatedGeneticParameterStandardError = computeGeneticParameterStandardError(updatedSsResiduals,
      updatedSsDeviations, updatedNumSamples)
    val updatedResidualDegreesOfFreedom = updateResidualDegreesOfFreedom(variantModel.association.numSamples)
    val updatedtStatistic = calculateTStatistic(updatedWeights, updatedGeneticParameterStandardError)
    val updatedPValue = calculatePValue(updatedtStatistic, updatedResidualDegreesOfFreedom)
    constructVariantModel(this.uniqueID,
      updatedSsDeviations,
      updatedSsResiduals,
      updatedGeneticParameterStandardError,
      updatedtStatistic,
      updatedResidualDegreesOfFreedom,
      updatedPValue,
      updatedWeights,
      updatedNumSamples)
    // TODO: implement dominant version of linear model
    //      case domLin: DominantLinearVariantModel => DominantLinearVariantModel(this.variantId,
    //        updatedSsDeviations,
    //        updatedSsResiduals,
    //        updatedGeneticParameterStandardError,
    //        updatedtStatistic,
    //        updatedResidualDegreesOfFreedom,
    //        updatedPValue,
    //        this.variant,
    //        updatedWeights,
    //        this.haplotypeBlock,
    //        updatedNumSamples)
  }

  /**
   * Returns updated sum of squared deviations from the mean of the genotype at that site
   * by adding the sum of squared deviations from the batch to the sum of squared
   * deviations of the existing model.
   *
   * @note The mean used in the calculation of the sum of squared deviations in the batch
   *       is the batch mean, not the global mean, since this enables a cleaner equation
   *       when approximating genetic parameter standard error in the update.
   *
   * @param batchSsDeviations The sum of squared deviations of the genotype values in
   *                          the batch from the batch mean.
   */
  def updateSsDeviations(batchSsDeviations: Double): Double = {
    association.ssDeviations + batchSsDeviations
  }

  /**
   * Returns updated sum of squared residuals for the model by adding the sum of squared
   * residuals for the batch to the sum of squared residuals of the existing model.
   *
   * @note The estimated value for the phenotype is estimated based on the batch-
   *       optimized model, not the global model.
   *
   * @param batchSsResiduals The sum of squared residuals for the batch
   */
  def updateSsResiduals(batchSsResiduals: Double): Double = {
    association.ssResiduals + batchSsResiduals
  }

  /**
   * Returns updated standard error of the genetic parameter using
   * updated values for the sum of squared residuals, sum of squared
   * deviations, and the number of samples.
   *
   * @note New standard error calculated based on updated values rather
   *       than taking an average of the batch standard error and the
   *       existing standard error (i.e. the method of update for
   *       the standard error in LogisticVariantModel's) in order to
   *       produce a closer approximation to the true standard error
   *       of the genetic parameter for the whole sample.
   *
   * @param updatedSsResiduals the result of updatesSsResiduals
   * @param updatedSsDeviations the result of updateSsDeviations
   * @param updatedNumSamples the result of updateNumSamples
   * @return Updated standard error for the genetic parameter in the model
   */
  def computeGeneticParameterStandardError(updatedSsResiduals: Double,
                                           updatedSsDeviations: Double,
                                           updatedNumSamples: Int): Double = {
    math.sqrt(((1.0 / (updatedNumSamples - association.weights.length)) * updatedSsResiduals) / (updatedSsDeviations))
  }

  /**
   * Returns updated geneticParameterDegreesOfFreedom for the model by adding
   * the number of samples in the batch to the existing value for degrees of
   * freedom in the model.
   *
   * @param batchNumSamples Number of samples in the batch used to update
   *
   * @return Updated degrees of freedom of the residual
   *
   */
  def updateResidualDegreesOfFreedom(batchNumSamples: Int): Int = {
    association.residualDegreesOfFreedom + batchNumSamples
  }

  /**
   * Returns t-statistic by taking the ratio of the weight associated with
   * the genetic parameter and the provided standard error of the genetic parameter.
   *
   * @param weights Array of doubles representing the model weights
   * @param geneticParameterStandardError Value for standard error of
   *                                             genetic parameter.
   * @return T-statistic for genetic parameter
   */
  def calculateTStatistic(weights: List[Double],
                          geneticParameterStandardError: Double): Double = {
    weights(1) / geneticParameterStandardError
  }

  /**
   * Returns p-value for linear model given t-statistic and degrees of freedom
   * of the residual.
   *
   * @param tStatistic Value for t-statistic
   * @param residualDegreesOfFreedom Degrees of freedom to use in t-distribution
   *
   * @return P-value, given a t-statistic and degrees of freedom for
   *         t-distribution
   */
  def calculatePValue(tStatistic: Double,
                      residualDegreesOfFreedom: Int): Double = {
    val tDist = new TDistribution(residualDegreesOfFreedom)
    val pvalue = 2 * tDist.cumulativeProbability(-math.abs(tStatistic))
    pvalue
  }

  def constructVariantModel(variantID: String,
                            updatedSsDeviations: Double,
                            updatedSsResiduals: Double,
                            updatedGeneticParameterStandardError: Double,
                            updatedtStatistic: Double,
                            updatedResidualDegreesOfFreedom: Int,
                            updatedPValue: Double,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): VM

}
