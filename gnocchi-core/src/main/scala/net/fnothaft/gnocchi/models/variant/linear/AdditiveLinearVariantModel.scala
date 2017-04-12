package net.fnothaft.gnocchi.models.variant.linear

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearRegression
import net.fnothaft.gnocchi.rdd.association.AdditiveLinearAssociation
import org.bdgenomics.formats.avro.Variant

case class AdditiveLinearVariantModel(variantId: String,
                                      ssDeviations: Double,
                                      ssResiduals: Double,
                                      geneticParameterStandardError: Double,
                                      tStatistic: Double,
                                      residualDegreesOfFreedom: Int,
                                      pValue: Double,
                                      variant: Variant,
                                      weights: List[Double],
                                      numSamples: Int,
                                      phenotype: String,
                                      haplotypeBlock: String = "")
    extends LinearVariantModel[AdditiveLinearVariantModel]
    with AdditiveLinearRegression with Serializable {

  type VM = AdditiveLinearVariantModel
  val modelType = "Additive Linear Variant Model"
  val regressionName = "Additive Linear Regression"

  /**
   * Updates the AdditiveLinearVariantModel given a new batch of data
   *
   * @param observations Array containing data at the particular site for
   *                     all samples. Format of each element is:
   *                     (gs, Array(pheno, covar1, ... covarp))
   *                     where gs is the diploid genotype at that site for the
   *                     given sample [0, 1, or 2], pheno is the sample's value for
   *                     the primary phenotype being regressed on, and covar1-covarp
   *                     are that sample's values for each covariate.
   */
  def update(observations: Array[(Double, Array[Double])]): AdditiveLinearVariantModel = {
    val batchVariantModel = applyToSite(observations, variant, phenotype)
      .toVariantModel
    mergeWith(batchVariantModel)
  }

  def constructVariantModel(variantID: String,
                            updatedSsDeviations: Double,
                            updatedSsResiduals: Double,
                            updatedGeneticParameterStandardError: Double,
                            updatedtStatistic: Double,
                            updatedResidualDegreesOfFreedom: Int,
                            updatedPValue: Double,
                            variant: Variant,
                            updatedWeights: List[Double],
                            updatedNumSamples: Int): AdditiveLinearVariantModel = {
    AdditiveLinearVariantModel(variantID,
      updatedSsDeviations,
      updatedSsResiduals,
      updatedGeneticParameterStandardError,
      updatedtStatistic,
      updatedResidualDegreesOfFreedom,
      updatedPValue,
      variant,
      updatedWeights,
      updatedNumSamples,
      phenotype)
  }

}
