package org.bdgenomics.gnocchi.api.java

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.{ QualityControlVariantModel, LogisticVariantModel }
import org.bdgenomics.gnocchi.models.{ LogisticGnocchiModelFactory, GnocchiModel, LogisticGnocchiModel, GnocchiModelMetaData }
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession

import scala.collection.JavaConversions._

object JavaLogisticGnocchiModelFactory {

  var gs: GnocchiSession = null

  def generate(gs: GnocchiSession) { this.gs = gs }

  def apply(genotypes: Dataset[CalledVariant],
            phenotypes: scala.collection.immutable.Map[java.lang.String, Phenotype],
            phenotypeNames: java.util.List[java.lang.String], // Option becomes raw object java.util.ArrayList[java.lang.String],
            QCVariantIDs: java.util.List[java.lang.String], // Option becomes raw object
            QCVariantSamplingRate: java.lang.Double,
            allelicAssumption: java.lang.String,
            validationStringency: java.lang.String): LogisticGnocchiModel = {

    // Convert python compatible nullable types to scala options
    val phenotypeNamesOption = if (phenotypeNames == null) {
      None
    } else {
      val phenotypeNamesList = asScalaBuffer(phenotypeNames).toList
      Some(phenotypeNamesList)
    }

    val QCVariantIDsOption = if (QCVariantIDs == null) {
      None
    } else {
      val QCVariantIDsList = asScalaBuffer(QCVariantIDs).toSet
      Some(QCVariantIDsList)
    }

    LogisticGnocchiModelFactory(genotypes,
      this.gs.sparkSession.sparkContext.broadcast(phenotypes),
      phenotypeNamesOption,
      QCVariantIDsOption,
      QCVariantSamplingRate,
      allelicAssumption,
      validationStringency)
  }
}

class JavaLogisticGnocchiModel(val lgm: LogisticGnocchiModel) {
  def mergeGnocchiModel(otherModel: JavaLogisticGnocchiModel): JavaLogisticGnocchiModel = {
    val newModel = lgm.mergeGnocchiModel(otherModel.lgm).asInstanceOf[LogisticGnocchiModel]
    new JavaLogisticGnocchiModel(newModel)
  }

  def mergeVariantModels(newVariantModels: Dataset[LogisticVariantModel]): Dataset[LogisticVariantModel] = {
    lgm.mergeVariantModels(newVariantModels)
  }

  def mergeQCVariants(newQCVariantModels: Dataset[QualityControlVariantModel[LogisticVariantModel]]): Dataset[CalledVariant] = {
    lgm.mergeQCVariants(newQCVariantModels)
  }

  def getVariantModels(): Dataset[LogisticVariantModel] = {
    lgm.variantModels
  }

  def getQCVariants(): Dataset[QualityControlVariantModel[LogisticVariantModel]] = {
    lgm.QCVariantModels
  }

  def getModelMetadata(): GnocchiModelMetaData = {
    lgm.metaData
  }

  def getModelType(): java.lang.String = {
    lgm.metaData.modelType
  }

  def getPhenotype(): java.lang.String = {
    lgm.metaData.phenotype
  }

  def getCovariates(): java.lang.String = {
    lgm.metaData.covariates
  }

  def getNumSamples(): java.lang.Integer = {
    lgm.metaData.numSamples
  }

  def getHaplotypeBlockErrorThreshold(): java.lang.Double = {
    lgm.metaData.haplotypeBlockErrorThreshold
  }

  def getFlaggedVariantModels(): java.util.List[java.lang.String] = {
    lgm.metaData.flaggedVariantModels.getOrElse(null)
  }

  def save(saveTo: java.lang.String): Unit = {
    lgm.save(saveTo)
  }
}
