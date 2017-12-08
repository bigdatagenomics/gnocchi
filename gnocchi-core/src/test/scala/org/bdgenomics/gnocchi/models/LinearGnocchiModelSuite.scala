package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.mockito.Mockito

import scala.collection.mutable

class LinearGnocchiModelSuite extends GnocchiFunSuite {
  ignore("Unit test of LGM.mergeVariantModels") {
    // (TODO) To unit test mergeVariantModels requires a seperate constructor that takes a mock of Dataset[LinearVariantModel] which is fairly messy
  }

  sparkTest("LinearGnocchiModel correctly combines GnocchiModels") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val observations = new Array[(Int, Int)](3)
    observations(0) = (10, 8)
    observations(1) = (8, 6)
    observations(2) = (10, 7)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val linearGnocchiModel = LinearGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val observationsSecond = new Array[(Int, Int)](3)
    observationsSecond(0) = (23, 4)
    observationsSecond(1) = (29, 3)
    observationsSecond(2) = (32, 2)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val linearGnocchiModelSecond = LinearGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))
    val oldMetadata = linearGnocchiModel.metaData
    val newMetadata = GnocchiModelMetaData(
      oldMetadata.modelType,
      oldMetadata.phenotype,
      oldMetadata.covariates,
      2,
      oldMetadata.haplotypeBlockErrorThreshold,
      oldMetadata.flaggedVariantModels)

    val mergedModel = linearGnocchiModel.mergeGnocchiModel(linearGnocchiModelSecond)
    assert(mergedModel.metaData == newMetadata)
  }

  sparkTest("LinearGnocchiModel correctly combines greater than 2 GnocchiModels") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    // Create first LinearGnocchiModel
    val observations = new Array[(Int, Int)](3)
    observations(0) = (10, 8)
    observations(1) = (8, 6)
    observations(2) = (10, 7)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val linearGnocchiModelFirst = LinearGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    // Create second LinearGnocchiModel
    val observationsSecond = new Array[(Int, Int)](3)
    observationsSecond(0) = (23, 4)
    observationsSecond(1) = (29, 3)
    observationsSecond(2) = (32, 2)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val linearGnocchiModelSecond = LinearGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    // Create third LinearGnocchiModel
    val observationsThird = new Array[(Int, Int)](3)
    observationsThird(0) = (21, 5)
    observationsThird(1) = (30, 2)
    observationsThird(2) = (34, 1)

    val genotypeStatesThird = observationsThird.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvThird = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStatesThird)
    val cvDatasetThird = mutable.MutableList[CalledVariant](cvThird).toDS()

    val linearGnocchiModelThird = LinearGnocchiModelFactory.apply(cvDatasetThird, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    // Compute expected metadata for fully merged model
    val oldMetadata = linearGnocchiModelFirst.metaData
    val newMetadata = GnocchiModelMetaData(
      oldMetadata.modelType,
      oldMetadata.phenotype,
      oldMetadata.covariates,
      3,
      oldMetadata.haplotypeBlockErrorThreshold,
      oldMetadata.flaggedVariantModels)

    // Merge all three models together
    val mergedModel = linearGnocchiModelFirst.mergeGnocchiModel(linearGnocchiModelSecond)
    val finalMergedModel = mergedModel.mergeGnocchiModel(linearGnocchiModelThird)

    assert(finalMergedModel.metaData == newMetadata)
  }

  ignore("LinearGnocchiModel.mergeQCVariants correct combines variant samples") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    // Create First LinearGnocchiModel
    val observations = new Array[(Int, Int)](3)
    observations(0) = (10, 8)
    observations(1) = (8, 6)
    observations(2) = (13, 7)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStates)
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val linearGnocchiModel = LinearGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    // Create Second LinearGnocchiModel
    val observationsSecond = new Array[(Int, Int)](3)
    observationsSecond(0) = (23, 4)
    observationsSecond(1) = (29, 3)
    observationsSecond(2) = (32, 2)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val linearGnocchiModelSecond = LinearGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val mergedQCVariants = linearGnocchiModel.mergeQCVariants(linearGnocchiModelSecond.QCVariantModels)
    val verifyQCVariants = genotypeStates ++ genotypeStatesSecond

    assert(verifyQCVariants.toSet == mergedQCVariants.map(_.samples).collect.flatten.toSet)
  }
}
