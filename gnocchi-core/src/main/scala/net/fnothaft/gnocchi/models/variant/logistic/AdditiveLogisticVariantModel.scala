package net.fnothaft.gnocchi.models.variant.logistic

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLogisticAssociation
import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticVariantModel
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

/**
  * Created by Taner on 3/21/17.
  */
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
