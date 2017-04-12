package net.fnothaft.gnocchi.models.variant.logistic

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.rdd.association.{ AdditiveLogisticAssociation, Association, DominantLogisticAssociation }
import org.apache.commons.math3.distribution.ChiSquaredDistribution
import org.bdgenomics.formats.avro.Variant

trait LogisticVariantModel[VM <: LogisticVariantModel[VM]] extends VariantModel[VM] {

  /**
   * Returns updated LogisticVariantModel of correct subtype
   *
   * @param variantModel Variant model whose parameters are to be used to update
   *                     existing variant model.
   *
   * @return Returns updated LogisticVariantModel of correct subtype
   */
  def mergeWith(variantModel: VM): VM = {
    val updatedNumSamples = updateNumSamples(variantModel.numSamples)
    val updatedGeneticParameterStandardError = computeGeneticParameterStandardError(variantModel.geneticParameterStandardError, variantModel.numSamples)
    val updatedWeights = updateWeights(variantModel.weights, variantModel.numSamples)
    val updatedWaldStatistic = calculateWaldStatistic(updatedGeneticParameterStandardError, updatedWeights)
    val updatedPValue = calculatePvalue(updatedWaldStatistic)
    constructVariantModel(this.variantId,
      this.variant,
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
    (batchStandardError * batchNumSamples.toDouble + geneticParameterStandardError * numSamples.toDouble) / ((batchNumSamples + numSamples).toDouble)

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
                            variant: Variant,
                            updatedGeneticParameterStandardError: Double,
                            updatedPValue: Double,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): VM
}
