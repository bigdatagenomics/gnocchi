/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.{ Column, Dataset, SparkSession }
import org.bdgenomics.gnocchi.models.variant.LinearVariantModel
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._

import scala.collection.mutable

class LinearGnocchiModelSuite extends GnocchiFunSuite {

  sparkTest("LinearGnocchiModel.mergeVariantModels correctly merges models together.") {
    val ss = sc.sparkSession
    import ss.implicits._

    val variantModel1 =
      LinearVariantModel(
        "rs1085962",
        17,
        145908437,
        "G",
        "A",
        94,
        4,
        Array(94.0, 110.0, 3271.4475914762697, 2302.674589586702, 110.0, 174.0, 3842.5031357490566, 2780.151112084831, 3271.4475914762697, 3842.5031357490566, 115884.84629517219, 80189.97753758742, 2302.674589586702, 2780.151112084831, 80189.97753758742, 58587.956330169975),
        Array(5617.461892639065, 6558.44291966915, 194976.5580018678, 137567.54574942816),
        90,
        List(69.07776644363027, -0.24898087761360677, -0.2572711819891003, -0.0029578942161391356))

    val variantModel2 =
      LinearVariantModel(
        "rs1085962",
        17,
        145908437,
        "G",
        "A",
        96,
        4,
        Array(96.0, 105.0, 3307.425151547912, 2395.4060317079206, 105.0, 163.0, 3592.261274997086, 2611.324498197782, 3307.425151547912, 3592.261274997086, 116417.75823673896, 82347.61698038159, 2395.4060317079206, 2611.324498197782, 82347.61698038159, 62444.74412145208),
        Array(5797.654008575947, 6300.23670069193, 199946.87776767055, 145067.2821012401),
        92,
        List(54.43723939165843, -0.7775917571858606, 0.0859282586467727, 0.1540969976355187))

    val mergedVariantModel =
      LinearVariantModel(
        "rs1085962",
        17,
        145908437,
        "G",
        "A",
        190,
        4,
        Array(190.0, 215.0, 6578.872743024182, 4698.080621294623, 215.0, 337.0, 7434.764410746142, 5391.475610282612, 6578.872743024182, 7434.764410746142, 232302.60453191114, 162537.594517969, 4698.080621294623, 5391.475610282612, 162537.594517969, 121032.70045162205),
        Array(11415.115901215013, 12858.67962036108, 394923.43576953834, 282634.8278506682),
        186,
        List(61.258606850236056, -0.7001667498483775, -0.07267615668330284, 0.08612956434272387))

    val linearGnocchiModel = mock(classOf[LinearGnocchiModel])
    val spyVar1 = spy(variantModel1)

    val varModelDataset1 = ss.createDataset(List(spyVar1))

    when(linearGnocchiModel.variantModels).thenReturn(varModelDataset1)
    when(linearGnocchiModel.mergeVariantModels(any[Dataset[LinearVariantModel]])).thenCallRealMethod()

    val mergedInGM = linearGnocchiModel.mergeVariantModels(ss.createDataset(List(variantModel2))).collect().head

    assert(mergedInGM.uniqueID == mergedVariantModel.uniqueID, "uniqueID in hand merged Variant Model different than uniqueID from the gnocchiModel merge.")
    assert(mergedInGM.chromosome == mergedVariantModel.chromosome, "chromosome in hand merged Variant Model different than chromosome from the gnocchiModel merge.")
    assert(mergedInGM.position == mergedVariantModel.position, "position in hand merged Variant Model different than position from the gnocchiModel merge.")
    assert(mergedInGM.referenceAllele == mergedVariantModel.referenceAllele, "referenceAllele in hand merged Variant Model different than referenceAllele from the gnocchiModel merge.")
    assert(mergedInGM.alternateAllele == mergedVariantModel.alternateAllele, "alternateAllele in hand merged Variant Model different than alternateAllele from the gnocchiModel merge.")
    assert(mergedInGM.numSamples == mergedVariantModel.numSamples, "numSamples in hand merged Variant Model different than numSamples from the gnocchiModel merge.")
    assert(mergedInGM.numPredictors == mergedVariantModel.numPredictors, "numPredictors in hand merged Variant Model different than numPredictors from the gnocchiModel merge.")
    assert(mergedInGM.xTx.toList == mergedVariantModel.xTx.toList, "xTx in hand merged Variant Model different than xTx from the gnocchiModel merge.")
    assert(mergedInGM.xTy.toList == mergedVariantModel.xTy.toList, "xTy in hand merged Variant Model different than xTy from the gnocchiModel merge.")
    assert(mergedInGM.residualDegreesOfFreedom == mergedVariantModel.residualDegreesOfFreedom, "residualDegreesOfFreedom in hand merged Variant Model different than residualDegreesOfFreedom from the gnocchiModel merge.")
    assert(mergedInGM.weights == mergedVariantModel.weights, "weights in hand merged Variant Model different than weights from the gnocchiModel merge.")
  }

  sparkTest("LinearGnocchiModel.mergeGnocchiModel breaks if there are overlapping sampleIDs between the two models being merged.") {
    val variantModels1 = mock(classOf[Dataset[LinearVariantModel]])
    val variantModels2 = mock(classOf[Dataset[LinearVariantModel]])
    val phenotypeName = "pheno_1"
    val covariateNames = List("covar_1", "covar_2")

    val sampleUIDs1 = Set("1", "2", "3", "4")
    val sampleUIDs2 = Set("3", "4", "5", "6")

    val numSamples = 4
    val allelicAssumption = "ADDITIVE"

    val model1 = LinearGnocchiModel(variantModels1, phenotypeName, covariateNames, sampleUIDs1, numSamples, allelicAssumption)
    val model2 = LinearGnocchiModel(variantModels2, phenotypeName, covariateNames, sampleUIDs2, numSamples, allelicAssumption)

    try {
      model1.mergeGnocchiModel(model2)
      fail("LinearGnocchiModel.mergeGnocchiModel allows overlapping samples! This can cause a model merge between overlapping datasets.")
    } catch {
      case a: java.lang.NullPointerException     => fail("LinearGnocchiModel.mergeGnocchiModel allows differing allelic assumptions! This can cause a model merge between models that have treated genotype states differently in their regressions.")
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("LinearGnocchiModel.mergeGnocchiModel breaks if the allelic assumption between the two models is different.") {
    val variantModels1 = mock(classOf[Dataset[LinearVariantModel]])
    val variantModels2 = mock(classOf[Dataset[LinearVariantModel]])
    val phenotypeName = "pheno_1"
    val covariateNames = List("covar_1", "covar_2")

    val sampleUIDs1 = Set("1", "2", "3", "4")
    val sampleUIDs2 = Set("5", "6", "7", "8")

    val numSamples = 4

    val model1 = LinearGnocchiModel(variantModels1, phenotypeName, covariateNames, sampleUIDs1, numSamples, "ADDITIVE")
    val model2 = LinearGnocchiModel(variantModels2, phenotypeName, covariateNames, sampleUIDs2, numSamples, "DOMINANT")

    try {
      model1.mergeGnocchiModel(model2)
      fail("LinearGnocchiModel.mergeGnocchiModel allows differing allelic assumptions! This can cause a model merge between models that have treated genotype states differently in their regressions.")
    } catch {
      case a: java.lang.NullPointerException     => fail("LinearGnocchiModel.mergeGnocchiModel allows differing allelic assumptions! This can cause a model merge between models that have treated genotype states differently in their regressions.")
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("LinearGnocchiModel.mergeGnocchiModel breaks by default if the phenotype used in each model is different.") {
    val variantModels1 = mock(classOf[Dataset[LinearVariantModel]])
    val variantModels2 = mock(classOf[Dataset[LinearVariantModel]])
    val covariateNames = List("covar_1", "covar_2")

    val sampleUIDs1 = Set("1", "2", "3", "4")
    val sampleUIDs2 = Set("5", "6", "7", "8")

    val numSamples = 4
    val allelicAssumption = "ADDITIVE"

    val model1 = LinearGnocchiModel(variantModels1, "pheno_1", covariateNames, sampleUIDs1, numSamples, allelicAssumption)
    val model2 = LinearGnocchiModel(variantModels2, "pheno_2", covariateNames, sampleUIDs2, numSamples, allelicAssumption)

    try {
      model1.mergeGnocchiModel(model2)
      fail("LinearGnocchiModel.mergeGnocchiModel allows differing phenotype names! This can cause conflicts between the primary phenotype being tested against.")
    } catch {
      case a: java.lang.NullPointerException     => fail("LinearGnocchiModel.mergeGnocchiModel allows differing phenotype names! This can cause conflicts between the primary phenotype being tested against.")
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("LinearGnocchiModel.mergeGnocchiModel allows for different phenotype names in the built gnocchi models") {
    val variantModels1 = mock(classOf[Dataset[LinearVariantModel]])
    val variantModels2 = mock(classOf[Dataset[LinearVariantModel]])
    val covariateNames = List("covar_1", "covar_2")

    val sampleUIDs1 = Set("1", "2", "3", "4")
    val sampleUIDs2 = Set("5", "6", "7", "8")

    val numSamples = 4
    val allelicAssumption = "ADDITIVE"

    val model1 = LinearGnocchiModel(variantModels1, "pheno_1", covariateNames, sampleUIDs1, numSamples, allelicAssumption)
    val model2 = LinearGnocchiModel(variantModels2, "pheno_2", covariateNames, sampleUIDs2, numSamples, allelicAssumption)

    try {
      model1.mergeGnocchiModel(model2, allowDifferentPhenotype = true)
    } catch {
      case a: java.lang.NullPointerException     =>
      case e: java.lang.IllegalArgumentException => fail("LinearGnocchiModel.mergeGnocchiModel doesn't allows differing phenotype names when parameter is passed! This can cause conflicts between the primary phenotype being tested against.")
    }
  }

  sparkTest("LinearGnocchiModel.mergeGnocchiModel correctly merges model metadata") {
    val variantModels1 = mock(classOf[Dataset[LinearVariantModel]])
    val variantModels2 = mock(classOf[Dataset[LinearVariantModel]])
    val phenotypeName = "pheno_1"
    val covariateNames = List("covar_1", "covar_2")

    val sampleUIDs1 = Set("1", "2", "3", "4")
    val sampleUIDs2 = Set("5", "6", "7", "8")

    val numSamples = 4
    val allelicAssumption = "ADDITIVE"

    val mockedLinearGnocchiModel = mock(classOf[LinearGnocchiModel])

    when(mockedLinearGnocchiModel.mergeVariantModels(any[Dataset[LinearVariantModel]]))
      .thenReturn(mock(classOf[Dataset[LinearVariantModel]]))
    when(mockedLinearGnocchiModel.mergeGnocchiModel(any[LinearGnocchiModel], anyBoolean())).thenCallRealMethod()

    when(mockedLinearGnocchiModel.variantModels).thenReturn(variantModels1)
    when(mockedLinearGnocchiModel.phenotypeName).thenReturn(phenotypeName)
    when(mockedLinearGnocchiModel.covariatesNames).thenReturn(covariateNames)
    when(mockedLinearGnocchiModel.sampleUIDs).thenReturn(sampleUIDs1)
    when(mockedLinearGnocchiModel.numSamples).thenReturn(numSamples)
    when(mockedLinearGnocchiModel.allelicAssumption).thenReturn(allelicAssumption)

    val model2 = LinearGnocchiModel(variantModels2, phenotypeName, covariateNames, sampleUIDs2, numSamples, allelicAssumption)

    val merged = mockedLinearGnocchiModel.mergeGnocchiModel(model2)

    assert(merged.sampleUIDs == Set("1", "2", "3", "4", "5", "6", "7", "8"), "Merging gnocchi models incorrectly merges sampleIDs")
    assert(merged.numSamples == 8, "Merging gnocchi models incorrectly merges number of samples")
    assert(merged.allelicAssumption == "ADDITIVE", "Merging gnocchi models incorrectly merges number of samples")
    assert(merged.phenotypeName == "pheno_1", "Merging gnocchi models incorrectly merges phenotype name")
    assert(merged.covariatesNames == List("covar_1", "covar_2"))
  }

  sparkTest("LinearGnocchiModel.save correctly saves a LinearGnocchiModel") {

  }
}
