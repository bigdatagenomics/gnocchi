package net.fnothaft.gnocchi.models.variant.logistic

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.rdd.association.Association

/**
  * Created by Taner on 3/21/17.
  */
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
