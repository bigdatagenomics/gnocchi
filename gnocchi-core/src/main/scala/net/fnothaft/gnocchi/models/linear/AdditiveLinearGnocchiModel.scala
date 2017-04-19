package net.fnothaft.gnocchi.models.linear

import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearRegression
import net.fnothaft.gnocchi.models._
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Variant

case class AdditiveLinearGnocchiModel(metaData: GnocchiModelMetaData,
                                      variantModels: RDD[AdditiveLinearVariantModel],
                                      comparisonVariantModels: RDD[(AdditiveLinearVariantModel, Array[(Double, Array[Double])])])
    extends GnocchiModel[AdditiveLinearVariantModel, AdditiveLinearGnocchiModel] {

  def constructGnocchiModel(metaData: GnocchiModelMetaData,
                            variantModels: RDD[AdditiveLinearVariantModel],
                            comparisonVariantModels: RDD[(AdditiveLinearVariantModel, Array[(Double, Array[Double])])]): AdditiveLinearGnocchiModel = {
    AdditiveLinearGnocchiModel(metaData, variantModels, comparisonVariantModels)
  }

  // calls the appropriate version of BuildVariantModel
  def regress(obs: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String): AdditiveLinearVariantModel = {
    AdditiveLinearRegression.applyToSite(obs, variant, metaData.phenotype)
      .toVariantModel
  }
}
