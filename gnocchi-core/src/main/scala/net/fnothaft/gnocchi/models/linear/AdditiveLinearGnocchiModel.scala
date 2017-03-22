package net.fnothaft.gnocchi.models.linear

import net.fnothaft.gnocchi.gnocchiModel.BuildAdditiveLinearVariantModel
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import org.apache.spark.rdd.RDD

/**
  * Created by Taner on 3/21/17.
  */
case class AdditiveLinearGnocchiModel(metaData: GnocchiModelMetaData,
                                      variantModels: RDD[VariantModel],
                                      comparisonVariantModels: RDD[(VariantModel, Array[(Double, Array[Double])])])
                                      extends GnocchiModel with Additive {

  // calls the appropriate version of BuildVariantModel
  def buildVariantModel(varModel: VariantModel,
                        obs: Array[(Double, Array[Double])]): AdditiveLinearVariantModel = {
    val variant = varModel.variant
    BuildAdditiveLinearVariantModel(obs, variant, metaData.phenotype)
  }

}
