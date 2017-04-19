package net.fnothaft.gnocchi.models.variant.logistic

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLogisticRegression
import net.fnothaft.gnocchi.rdd.association.AdditiveLogisticAssociation
import org.bdgenomics.formats.avro.Variant

case class AdditiveLogisticVariantModel(variantId: String,
                                        variant: Variant,
                                        weights: List[Double],
                                        geneticParameterStandardError: Double,
                                        pValue: Double,
                                        numSamples: Int,
                                        phenotype: String,
                                        haplotypeBlock: String = "")
    extends LogisticVariantModel[AdditiveLogisticVariantModel]
    with AdditiveLogisticRegression with Serializable {

  val modelType = "Additive Logistic Variant Model"
  override val regressionName = "Additive Logistic Regression"

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
  def update(observations: Array[(Double, Array[Double])]): AdditiveLogisticVariantModel = {
    val batchVariantModel = applyToSite(observations, variant, phenotype)
      .toVariantModel
    mergeWith(batchVariantModel)
  }
  /**
   * Returns new Association object with provided values for
   * for weights, geneticParameterStandardError,
   * number of samples, and pValue.
   *
   * @note using updateAssociation enables enforcement that all of the
   *       fields required for a LogisticVariantModel are present in the
   *       new association object.
   *
   * @param geneticParameterStandardError Updated standard error of genetic parameter
   *                                      in regression model
   * @param pValue Updated P value of the genetic parameter in the regression model
   * @param numSamples Number of samples in the updated model
   * @param weights Weights associated with the updated regression model
   * @return Returns new Association object with updated parameters.
   */
  def updateAssociation(geneticParameterStandardError: Double,
                        pValue: Double,
                        numSamples: Int,
                        weights: Array[Double]): AdditiveLogisticAssociation = {
    AdditiveLogisticAssociation("NoID", 0, "NoModelType", Array(0, 0, 0, 0), 0.0, new Variant(), "NoPheno", 0.0, 0.0, Map())
  }

  def constructVariantModel(variantId: String,
                            variant: Variant,
                            updatedGeneticParameterStandardError: Double,
                            updatedPValue: Double,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): AdditiveLogisticVariantModel = {
    AdditiveLogisticVariantModel(variantId, variant, updatedWeights, updatedGeneticParameterStandardError, updatedPValue, updatedNumSamples, phenotype)
  }
}
