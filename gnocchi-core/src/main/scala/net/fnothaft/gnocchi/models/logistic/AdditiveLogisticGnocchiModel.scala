package net.fnothaft.gnocchi.models.logistic

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLogisticRegression
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant

case class AdditiveLogisticGnocchiModel(metaData: GnocchiModelMetaData,
                                        variantModels: RDD[AdditiveLogisticVariantModel],
                                        comparisonVariantModels: RDD[(AdditiveLogisticVariantModel, Array[(Double, Array[Double])])])
    extends GnocchiModel[AdditiveLogisticVariantModel, AdditiveLogisticGnocchiModel] {

  def constructGnocchiModel(metaData: GnocchiModelMetaData,
                            variantModels: RDD[AdditiveLogisticVariantModel],
                            comparisonVariantModels: RDD[(AdditiveLogisticVariantModel, Array[(Double, Array[Double])])]): AdditiveLogisticGnocchiModel = {
    AdditiveLogisticGnocchiModel(metaData, variantModels, comparisonVariantModels)
  }

  // calls the appropriate version of BuildVariantModel
  def regress(obs: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String): AdditiveLogisticVariantModel = {
    AdditiveLogisticRegression.applyToSite(obs, variant, metaData.phenotype)
      .toVariantModel
  }
}
