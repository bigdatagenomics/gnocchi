package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.SparkSession
import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.mockito.Mockito

import scala.collection.mutable

class LogisticGnocchiModelSuite extends GnocchiFunSuite {
  sparkTest("Unit test of LGM.mergeVariantModels") {
    // (TODO) To unit test mergeVariantModels requires a seperate constructor that takes a mock of Dataset[LogisticVariantModel] which is fairly messy
  }

  sparkTest("LogisticGnocchiModel correctly combines GnocchiModels") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val observations = new Array[(Int, Int)](3)
    observations(0) = (10, 8)
    observations(1) = (8, 6)
    observations(2) = (13, 7)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cv = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStates)
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val logisticGnocchiModel = LogisticGnocchiModelFactory.apply(cvDataset, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val observationsSecond = new Array[(Int, Int)](3)
    observationsSecond(0) = (23, 4)
    observationsSecond(1) = (29, 3)
    observationsSecond(2) = (32, 2)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val logisticGnocchiModelSecond = LogisticGnocchiModelFactory.apply(cvDatasetSecond, sc.broadcast(phenoMap), Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))
    val oldMetadata = logisticGnocchiModel.metaData
    val newMetadata = GnocchiModelMetaData(
      oldMetadata.modelType,
      oldMetadata.phenotype,
      oldMetadata.covariates,
      2,
      oldMetadata.haplotypeBlockErrorThreshold,
      oldMetadata.flaggedVariantModels)

    val mergedModel = logisticGnocchiModel.mergeGnocchiModel(logisticGnocchiModelSecond)
    assert(mergedModel.metaData == newMetadata)
    // mergeGnocchiModel::mergeVariantModels tested in LogisticVariantModelSuite
    // mergeGnocchiModel::mergeQCVariants tested below
  }

  // (TODO) Need to create a singular matrix
  ignore("LogisticGnocchiModel.mergeQCVariants correct combines variant samples") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val observations = new Array[(Int, Int)](3)
    observations(0) = (10, 8)
    observations(1) = (8, 6)
    observations(2) = (13, 7)

    val genotypeStates = observations.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val gs = createSampleGenotypeStates(num = 10, maf = 0.35, geno = 0.0, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val cvDataset = mutable.MutableList[CalledVariant](cv).toDS()

    val phenoMap = observations.map(_._2)
      .toList
      .zipWithIndex
      .map(item => (item._2.toString, Phenotype(item._2.toString, "pheno1", item._1)))
      .toMap

    val phenos = sc.broadcast(createSamplePhenotype(calledVariant = Option(cv), numCovariate = 10))

    val logisticGnocchiModel = LogisticGnocchiModelFactory.apply(cvDataset, phenos, Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val observationsSecond = new Array[(Int, Int)](3)
    observationsSecond(0) = (23, 4)
    observationsSecond(1) = (29, 3)
    observationsSecond(2) = (32, 2)

    val genotypeStatesSecond = observationsSecond.map(_._1).toList.zipWithIndex.map(item => GenotypeState(item._2.toString, item._1.toString))
    val cvSecond = CalledVariant(1, 1, "rs123456", "A", "C", "", "", "", "", genotypeStatesSecond)
    val cvDatasetSecond = mutable.MutableList[CalledVariant](cvSecond).toDS()

    val logisticGnocchiModelSecond = LogisticGnocchiModelFactory.apply(cvDataset, phenos, Option.apply(List[String]("pheno1")), Option.apply(List[String]("rs123456").toSet))

    val mergedQCVariants = logisticGnocchiModel.mergeQCVariants(logisticGnocchiModelSecond.QCVariantModels)
    val verifyQCVariants = genotypeStates ++ genotypeStatesSecond

    assert(verifyQCVariants.toSet == mergedQCVariants.map(_.samples).collect.flatten.toSet)
  }
}
