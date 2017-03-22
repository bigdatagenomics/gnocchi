package net.fnothaft.gnocchi.models.variant.linear

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.rdd.association.Association
import org.bdgenomics.formats.avro.Variant

/**
  * Created by Taner on 3/21/17.
  */
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
