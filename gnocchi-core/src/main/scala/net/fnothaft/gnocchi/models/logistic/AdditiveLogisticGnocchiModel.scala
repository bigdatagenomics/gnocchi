package net.fnothaft.gnocchi.models.logistic

import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLogisticVariantModel
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
import org.apache.spark.rdd.RDD


case class AdditiveLogisticGnocchiModel(metaData: GnocchiModelMetaData,
                                        variantModels: RDD[VariantModel],
                                        comparisonVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])])
                                        extends GnocchiModel with Additive {

  // calls the appropriate version of BuildVariantModel
  def buildVariantModel(varModel: VariantModel,
                        obs: Array[(Double, Array[Double])]): AdditiveLogisticVariantModel = {
    val variant = varModel.variant
    BuildAdditiveLogisticVariantModel(obs, variant, metaData.phenotype)
  }

}
