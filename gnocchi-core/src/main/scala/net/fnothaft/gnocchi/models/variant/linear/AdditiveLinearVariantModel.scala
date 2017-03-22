package net.fnothaft.gnocchi.models.variant.linear

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearAssociation
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLinearVariantModel
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.rdd.association.Association
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

/**
  * Created by Taner on 3/21/17.
  */
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
