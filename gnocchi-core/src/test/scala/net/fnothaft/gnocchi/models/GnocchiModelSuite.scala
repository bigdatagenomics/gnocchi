///**
// * Licensed to Big Data Genomics (BDG) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The BDG licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package net.fnothaft.gnocchi.models
//
//import net.fnothaft.gnocchi.GnocchiFunSuite
//import net.fnothaft.gnocchi.algorithms.siteregression.{ AdditiveLinearRegression, AdditiveLogisticRegression }
//import net.fnothaft.gnocchi.models.linear.AdditiveLinearGnocchiModel
//import net.fnothaft.gnocchi.models.logistic.AdditiveLogisticGnocchiModel
//import net.fnothaft.gnocchi.models.variant.logistic.AdditiveLogisticVariantModel
//import org.apache.commons.math3.linear.SingularMatrixException
//import org.apache.spark.SparkContext
//import org.bdgenomics.formats.avro._
//
//import collection.JavaConverters._
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//
//class GnocchiModelSuite extends GnocchiFunSuite {
//
//  def gnocchiModelFixture(sc: SparkContext) = new {
//    /* initialGroupGenotypes 3 variants, 5 samples
//#CHROM	POS	    ID	        REF	ALT	QUAL	FILTER	INFO	FORMAT	sample1	sample2	sample3	sample4	sample5
//1	      752721	rs3131971	  A	  G	  60	  PASS	  .	    GT	    0/0	    0/0	    0/0	    0/1	    1/0
//1	      752722	rs3131972	  A	  T	  60	  PASS	  .	    GT	    1/1	    1/1	    1/1	    1/1	    0/1
//1	      752723	rs3131973	  C	  G	  60	  PASS	  .	    GT	    0/1	    0/0	    0/0	    1/1	    1/0
//*/
//
//    /* initial Phenotype data
//    Sample  Primary_Phenotype Covariate0  Covariate1
//    sample1      1.4          0.8404      2.9080
//    sample2      0.1         -0.8880      0.8252
//    sample3      2.3          0.1001      1.3790
//    sample4      6.0         -0.5445     -1.0582
//    sample5      1.1          0.3035     -0.4686
//     */
//
//    //TODO: Figure out why we need to make Java objects in order to serialize Variant objects now...
//    val variant0 = Variant.newBuilder.build
//    variant0.setContigName("variant0")
//
//    val variant1 = Variant.newBuilder.build
//    variant1.setContigName("variant1")
//
//    val variant2 = Variant.newBuilder.build
//    variant2.setContigName("variant2")
//
//    val linearObs0 = Array((0.0, Array(1.4, 0.8404, 2.9080)),
//      (0.0, Array(0.1, -0.8880, 0.8252)),
//      (0.0, Array(2.3, 0.1001, 1.3790)),
//      (1.0, Array(6.0, -0.5445, -1.0582)),
//      (1.0, Array(1.1, 0.3035, -0.4686)))
//    val linearObs1 = Array((2.0, Array(1.4, 0.8404, 2.9080)),
//      (2.0, Array(0.1, -0.8880, 0.8252)),
//      (2.0, Array(2.3, 0.1001, 1.3790)),
//      (2.0, Array(6.0, -0.5445, -1.0582)),
//      (1.0, Array(1.1, 0.3035, -0.4686)))
//    val linearObs2 = Array((1.0, Array(1.4, 0.8404, 2.9080)),
//      (0.0, Array(0.1, -0.8880, 0.8252)),
//      (0.0, Array(2.3, 0.1001, 1.3790)),
//      (2.0, Array(6.0, -0.5445, -1.0582)),
//      (1.0, Array(1.1, 0.3035, -0.4686)))
//    val linearNumSamples = 5
//    val linearHaplotypeBlockErrorThreshold = 0.5
//    val linearModelType = "Additive Linear"
//    val variables = "Genotype + 2 covariates"
//    val linearFlaggedVariantModels = List()
//    val linearPhenotype = "Phenotype"
//    val phaseSetId0 = 0
//    val phaseSetId1 = 1
//    val phaseSetId2 = 2
//
//    val linearVariantModel0 = AdditiveLinearRegression.applyToSite(
//      linearObs0, variant0, linearPhenotype, phaseSetId0).toVariantModel
//    val linearVariantModel1 = AdditiveLinearRegression.applyToSite(
//      linearObs1, variant1, linearPhenotype, phaseSetId1).toVariantModel
//    val linearVariantModel2 = AdditiveLinearRegression.applyToSite(
//      linearObs2, variant2, linearPhenotype, phaseSetId2).toVariantModel
//
//    /* updateGroupGenotypes 3 variants, 5 samples
//    #CHROM	POS	    ID	        REF	ALT	QUAL	FILTER	INFO	FORMAT	sample6	sample7	sample8	sample9	sample10
//    1	      752721	rs3131971	  A	  G	  60	  PASS	  .	    GT	    1/0	    1/1	    1/1	    1/1	    1/1
//    1	      752722	rs3131972	  A	  T	  60	  PASS	  .	    GT	    1/0	    1/0	    0/0	    0/0	    0/0
//    1	      752723	rs3131973	  C	  G	  60	  PASS	  .	    GT	    1/0	    1/1	    0/0	    1/1	    1/1
//     */
//    /* update Phenotype data
//    Sample  Primary_Phenotype Covariate0  Covariate1
//    sample6       2           -0.6003     -0.2725
//    sample7       2            0.4900      1.0984
//    sample8       3            0.7394     -0.2779
//    sample9       3            1.7119      0.7015
//    sample10      3           -0.1941     -2.0518
//    */
//
//    val linearObsForUpdate0 = Array((1.0, Array(2, -0.6003, -0.2725)),
//      (2.0, Array(2, 0.4900, 1.0984)),
//      (2.0, Array(3, 0.7394, -0.2779)),
//      (2.0, Array(3, 1.7119, 0.7015)),
//      (2.0, Array(3, -0.1941, -2.0518)))
//    val linearObsForUpdate1 = Array((1.0, Array(2, -0.6003, -0.2725)),
//      (1.0, Array(2, 0.4900, 1.0984)),
//      (0.0, Array(3, 0.7394, -0.2779)),
//      (0.0, Array(3, 1.7119, 0.7015)),
//      (0.0, Array(3, -0.1941, -2.0518)))
//    val linearObsForUpdate2 = Array((1.0, Array(2, -0.6003, -0.2725)),
//      (2.0, Array(2, 0.4900, 1.0984)),
//      (0.0, Array(3, 0.7394, -0.2779)),
//      (2.0, Array(3, 1.7119, 0.7015)),
//      (2.0, Array(3, -0.1941, -2.0518)))
//
//    /* Setting up Gnocchi Model */
//    val linearVariantModels = sc.parallelize(Array(linearVariantModel0, linearVariantModel1, linearVariantModel2))
//    val linearComparisonModels = sc.parallelize(Array((linearVariantModel0, linearObs0), (linearVariantModel1, linearObs1), (linearVariantModel2, linearObs2)))
//    val linearObservationsForUpdate = sc.parallelize(List((variant0, linearObsForUpdate0),
//      (variant1, linearObsForUpdate1), (variant2, linearObsForUpdate2)))
//    val linearMetaData = new GnocchiModelMetaData(linearNumSamples,
//      linearHaplotypeBlockErrorThreshold, linearModelType,
//      variables, linearFlaggedVariantModels, linearPhenotype)
//    val linearGnocchiModel = AdditiveLinearGnocchiModel(linearMetaData, linearVariantModels,
//      linearComparisonModels)
//    /* Done setting up Gnocchi Model */
//
//    val mergedLinearDataForComparisonModel = (linearObs0.toList ::: linearObsForUpdate0.toList)
//      .toArray
//
//    /* initialGroupGenotypes 3 variants, 5 samples
//    #CHROM	POS	    ID	        REF	ALT	QUAL	FILTER	INFO	FORMAT	sample1	sample2	sample3	sample4	sample5
//    1	      752721	rs3131971	  A	  G	  60	  PASS	  .	    GT	    0/0	    0/0	    0/0	    0/1	    1/0
//    1	      752722	rs3131972	  A	  T	  60	  PASS	  .	    GT	    1/1	    1/1	    1/1	    1/1	    0/1
//    1	      752723	rs3131973	  C	  G	  60	  PASS	  .	    GT	    0/1	    0/0	    0/0	    1/1	    1/0
//    */
//    /* Phenotype data
//    Sample  Primary_Phenotype Covariate0  Covariate1
//    sample1      1.0          0.8404      2.9080
//    sample2      0.0         -0.8880      0.8252
//    sample3      0.0          0.1001      1.3790
//    sample4      1.0         -0.5445     -1.0582
//    sample5      1.0          0.3035     -0.4686
//    */
//
//    val logisticObs0 = Array((0.0, Array(1.0, 0.8404, 2.9080)),
//      (0.0, Array(0.0, -0.8880, 0.8252)),
//      (0.0, Array(0.0, 0.1001, 1.3790)),
//      (1.0, Array(1.0, -0.5445, -1.0582)),
//      (1.0, Array(1.0, 0.3035, -0.4686)))
//    val logisticObs1 = Array((2.0, Array(1.0, 0.8404, 2.9080)),
//      (2.0, Array(0.0, -0.8880, 0.8252)),
//      (2.0, Array(0.0, 0.1001, 1.3790)),
//      (2.0, Array(1.0, -0.5445, -1.0582)),
//      (1.0, Array(1.0, 0.3035, -0.4686)))
//    val logisticObs2 = Array((1.0, Array(1.0, 0.8404, 2.9080)),
//      (0.0, Array(0.0, -0.8880, 0.8252)),
//      (0.0, Array(0.0, 0.1001, 1.3790)),
//      (2.0, Array(1.0, -0.5445, -1.0582)),
//      (1.0, Array(1.0, 0.3035, -0.4686)))
//
//    val logisticPhenotype = "Logistic Phenotype"
//
//    // TODO: make this not throw a singular matrix error
//    val logisticVariantModel0 = try {
//      AdditiveLogisticRegression.applyToSite(logisticObs0, variant0, logisticPhenotype, phaseSetId0)
//        .toVariantModel
//    } catch {
//      case error: SingularMatrixException => {
//        AdditiveLogisticRegression.constructAssociation(variant0.getContigName, 0, "", new Array[Double](4), 0.0, variant0, "", 0.0, 0.0, phaseSetId0, Map(("", ""))).toVariantModel
//      }
//    }
//    // TODO: make this not throw a singular matrix error
//    val logisticVariantModel1 = try {
//      AdditiveLogisticRegression.applyToSite(logisticObs1, variant1, logisticPhenotype, phaseSetId1)
//        .toVariantModel
//    } catch {
//      case error: SingularMatrixException => {
//        AdditiveLogisticRegression.constructAssociation(variant1.getContigName, 0, "", new Array[Double](4), 0.0, variant1, "", 0.0, 0.0, phaseSetId1, Map(("", ""))).toVariantModel
//      }
//    }
//
//    // TODO: make this not throw a singular matrix error
//    val logisticVariantModel2 = try {
//      AdditiveLogisticRegression.applyToSite(logisticObs2, variant2, logisticPhenotype, phaseSetId2)
//        .toVariantModel
//    } catch {
//      case error: SingularMatrixException => {
//        AdditiveLogisticRegression.constructAssociation(variant2.getContigName, 0, "", new Array[Double](4), 0.0, variant2, "", 0.0, 0.0, phaseSetId2, Map(("", ""))).toVariantModel
//      }
//    }
//
//    //    val logisticVariantModel1 = AdditiveLogisticRegression.applyToSite(logisticObs1, variant1, logisticPhenotype)
//    //      .toVariantModel
//    //    val logisticVariantModel2 = AdditiveLogisticRegression.applyToSite(logisticObs2, variant2, logisticPhenotype)
//    //      .toVariantModel
//
//    val numSamples = 5
//    val haplotypeBlockErrorThreshold = 0.5
//    val modelType = "Additive logistic"
//    val flaggedVariantModels = List()
//    val logisticMetaData = new GnocchiModelMetaData(numSamples, haplotypeBlockErrorThreshold, modelType,
//      variables, flaggedVariantModels, logisticPhenotype)
//
//    /* updateGroupGenotypes 3 variants, 5 samples
//    #CHROM	POS	    ID	        REF	ALT	QUAL	FILTER	INFO	FORMAT	sample6	sample7	sample8	sample9	sample10
//    1	      752721	rs3131971	  A	  G	  60	  PASS	  .	    GT	    1/0	    1/1	    1/1	    1/1	    1/1
//    1	      752722	rs3131972	  A	  T	  60	  PASS	  .	    GT	    1/0	    1/0	    0/0	    0/0	    0/0
//    1	      752723	rs3131973	  C	  G	  60	  PASS	  .	    GT	    1/0	    1/1	    0/0	    1/1	    1/1
//    */
//    /* Phenotype data
//    Sample  Primary_Phenotype Covariate0  Covariate1
//    sample6       1.0           -0.6003     -0.2725
//    sample7       1.0            0.4900      1.0984
//    sample8       0.0            0.7394     -0.2779
//    sample9       0.0            1.7119      0.7015
//    sample10      0.0           -0.1941     -2.0518
//    */
//
//    val logisticObsForUpdate0 = Array((1.0, Array(1.0, -0.6003, -0.2725)),
//      (2.0, Array(1.0, 0.4900, 1.0984)),
//      (2.0, Array(1.0, 0.7394, -0.2779)),
//      (2.0, Array(0.0, 1.7119, 0.7015)),
//      (2.0, Array(0.0, -0.1941, -2.0518)))
//    val logisticObsForUpdate1 = Array((1.0, Array(1.0, -0.6003, -0.2725)),
//      (1.0, Array(1.0, 0.4900, 1.0984)),
//      (0.0, Array(1.0, 0.7394, -0.2779)),
//      (0.0, Array(0.0, 1.7119, 0.7015)),
//      (0.0, Array(0.0, -0.1941, -2.0518)))
//    val logisticObsForUpdate2 = Array((1.0, Array(1.0, -0.6003, -0.2725)),
//      (2.0, Array(1.0, 0.4900, 1.0984)),
//      (0.0, Array(1.0, 0.7394, -0.2779)),
//      (2.0, Array(0.0, 1.7119, 0.7015)),
//      (2.0, Array(0.0, -0.1941, -2.0518)))
//
//    /* Sets up logistic Gnocchi Model */
//    val logisticVariantModels = sc.parallelize(Array(logisticVariantModel0, logisticVariantModel1, logisticVariantModel2))
//    val logisticComparisonModels = sc.parallelize(Array((logisticVariantModel0, logisticObs0), (logisticVariantModel1, logisticObs1), (logisticVariantModel2, logisticObs2)))
//    val observationsForUpdate = sc.parallelize(List((variant0, logisticObsForUpdate0),
//      (variant1, logisticObsForUpdate1), (variant2, logisticObsForUpdate2)))
//    val logisticGnocchiModel = AdditiveLogisticGnocchiModel(logisticMetaData, logisticVariantModels,
//      logisticComparisonModels)
//
//    val mergedLogisticDataForComparisonModel = (logisticObs0.toList ::: logisticObsForUpdate0.toList)
//      .toArray
//  }
//
//  sparkTest("Updated metadata should only differ from old metadata in numSamples " +
//    "and flaggedVariantModels") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val metaData = linearGnocchiModel.metaData
//    val updatedMetaData = linearGnocchiModel.updateMetaData(5, List("NewFlaggedVariant"))
//    assert(updatedMetaData.numSamples === metaData.numSamples + 5,
//      "Number of samples in updated metadata incorrect after updateMetaData call.")
//
//    assert(updatedMetaData.modelType === metaData.modelType,
//      "Model type in updated metaData did not match original.")
//
//    assert(updatedMetaData.haplotypeBlockErrorThreshold ===
//      metaData.haplotypeBlockErrorThreshold, "HaplotypeBlockErrorThreshold" +
//      "in updated metaData did not match original.")
//
//    assert(updatedMetaData.phenotype === metaData.phenotype, "Phenotype description " +
//      "in updated metaData did not match original.")
//
//    assert(updatedMetaData.variables ===
//      metaData.variables, "Variables description in updated metaData did not" +
//      "match original.")
//
//    assert(updatedMetaData.flaggedVariantModels ===
//      List("NewFlaggedVariant"), "List of flagged variants in updated metadata" +
//      "incorrect after update.")
//  }
//
//  sparkTest("UpdateVariantModels should update each linear variant model") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val updatedLinearVariantModels = linearGnocchiModel.updateVariantModels(linearObservationsForUpdate)
//
//    assert(updatedLinearVariantModels.count === linearGnocchiModel.variantModels.count(), "Number" +
//      "of variant models after updateVariantModel incorrect")
//
//    val variant0Model = linearGnocchiModel.variantModels.filter(_.variantId == "variant0").collect.head
//    val updatedVariant0Model = updatedLinearVariantModels.filter(_.variantId == "variant0").collect.head
//
//    assert(updatedVariant0Model.weights sameElements variant0Model.update(linearObsForUpdate0).weights)
//    assert(updatedVariant0Model == variant0Model.update(linearObsForUpdate0), "Variant model " +
//      "for variant0 incorrect after updateVariantModels call.")
//
//    val variant1Model = linearGnocchiModel.variantModels.filter(_.variantId == "variant1").collect.head
//    val updatedVariant1Model = updatedLinearVariantModels.filter(_.variantId == "variant1").collect.head
//    assert(updatedVariant1Model == variant1Model.update(linearObsForUpdate1), "Variant model " +
//      "for variant1 incorrect after updateVariantModels call.")
//
//    val variant2Model = linearGnocchiModel.variantModels.filter(_.variantId == "variant2").collect.head
//    val updatedVariant2Model = updatedLinearVariantModels.filter(_.variantId == "variant2").collect.head
//    assert(updatedVariant2Model == variant2Model.update(linearObsForUpdate2), "Variant model " +
//      "for variant2 incorrect after updateVariantModels call.")
//  }
//
//  sparkTest("updateComparisonVariantModels should return recomputed linear model " +
//    "and updated data") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val updatedLinearComparisonVariantModels = linearGnocchiModel.updateComparisonVariantModels(linearObservationsForUpdate)
//
//    assert(updatedLinearComparisonVariantModels.count() === linearGnocchiModel.comparisonVariantModels.count(),
//      "Number of comparison variant models after updatedComparisonModels incorrect.")
//
//    val variant = linearGnocchiModel.comparisonVariantModels.filter(_._1.variantId == "variant0")
//      .collect.head
//      ._1.variant
//    val comparisonVariantModel = AdditiveLinearRegression
//      .applyToSite(mergedLinearDataForComparisonModel, variant, linearPhenotype, phaseSetId0).toVariantModel
//
//    val (variantModelAfterUpdate, dataAfterUpdate) = updatedLinearComparisonVariantModels.filter(_._1.variantId == comparisonVariantModel.variantId).collect.head
//    val dataToLists = dataAfterUpdate.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//    assert(variantModelAfterUpdate === comparisonVariantModel,
//      "Comparison variant model for variant0 after updateComparisonModels call" +
//        " does not match expected variant model")
//    val mergedDataForComparisonToLists = mergedLinearDataForComparisonModel.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//    assert(dataToLists == mergedDataForComparisonToLists,
//      "Merged data in updated comparison model for variant0 did not match expected")
//  }
//
//  sparkTest("compareModels should flag variant0 if HaplotypeBlockErrorThreshold equals 0.5") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    // Update linear model with update group
//    val updatedLinearGnocchiModel = linearGnocchiModel.update(linearObservationsForUpdate)
//
//    //    println("thresh: " + updatedLinearGnocchiModel.metaData.haplotypeBlockErrorThreshold)
//    //    println("Comparison PValues: " + updatedLinearGnocchiModel.comparisonVariantModels.collect
//    //      .map(md => {
//    //        val (model, data) = md
//    //        (model.variantId, model.pValue)
//    //      }).toList)
//    //    println("VariantModel PValues: " + updatedLinearGnocchiModel.variantModels.collect
//    //      .map(m => {
//    //        (m.variantId, m.pValue)
//    //      }).toList)
//
//    val flaggedVariants = updatedLinearGnocchiModel.compareModels(updatedLinearGnocchiModel.variantModels,
//      updatedLinearGnocchiModel.comparisonVariantModels)
//    assert(flaggedVariants == List("variant0"), "Variant0 should be the only variant flagged if " +
//      "haplotypeBlockErrorThreshold is 0.5")
//  }
//
//  sparkTest("New linear gnocchi model from update should have updated metadata") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    // Update linear model with update group
//    val updatedLinearGnocchiModel = linearGnocchiModel.update(linearObservationsForUpdate)
//
//    assert(updatedLinearGnocchiModel.metaData.numSamples ===
//      linearGnocchiModel.metaData.numSamples + linearObsForUpdate0.length,
//      "Number of samples in gnocchi model did not match expected number of samples (10).")
//
//    assert(updatedLinearGnocchiModel.metaData.modelType === linearGnocchiModel.metaData.modelType,
//      "Model type of updated model did not match original model type.")
//
//    assert(updatedLinearGnocchiModel.metaData.haplotypeBlockErrorThreshold ===
//      linearGnocchiModel.metaData.haplotypeBlockErrorThreshold, "HaplotypeBlockErrorThreshold" +
//      "in updated model did not match original model.")
//
//    assert(updatedLinearGnocchiModel.metaData.phenotype ===
//      linearGnocchiModel.metaData.phenotype, "Phenotype description in updated model did not" +
//      "match phenotype description in original model.")
//
//    assert(updatedLinearGnocchiModel.metaData.variables ===
//      linearGnocchiModel.metaData.variables, "Variables description in updated model did not" +
//      "match variables description in original model.")
//
//    assert(updatedLinearGnocchiModel.metaData.flaggedVariantModels ===
//      List("variant0"))
//
//  }
//
//  sparkTest("New linear gnocchi model from update should have updated variantModels") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val updatedLinearVariantModels = linearGnocchiModel.updateVariantModels(linearObservationsForUpdate)
//
//    // Update linear model with update group
//    val updatedLinearGnocchiModel = linearGnocchiModel.update(linearObservationsForUpdate)
//
//    assert(updatedLinearGnocchiModel.variantModels.collect.toList == updatedLinearVariantModels.collect.toList,
//      "Variant Models rdd in updated linear gnocchi model did not match" +
//        "expected results from updateVariantModels(...)")
//  }
//
//  sparkTest("New linear gnocchi model from update should have updated comparisonVariantModels") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    // Update linear model with update group
//    val updatedLinearGnocchiModel = linearGnocchiModel.update(linearObservationsForUpdate)
//
//    val updateResult = linearGnocchiModel.updateComparisonVariantModels(linearObservationsForUpdate)
//
//    val (updatedVariantModel, updatedData) = updateResult.filter(_._1.variantId == "variant0").collect.head
//    val updatedDataToLists = updatedData.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//
//    val comparisonModelsAfterUpdate = updatedLinearGnocchiModel.comparisonVariantModels
//
//    val (variantModelAfterUpdate, dataAfterUpdate) = comparisonModelsAfterUpdate.filter(_._1.variantId == "variant0").collect.head
//    val dataAfterUpdateToLists = dataAfterUpdate.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//
//    assert(dataAfterUpdateToLists == updatedDataToLists,
//      "Merged data in updated comparison model for variant0 in " +
//        "updated linear gnocchi model did not match expected results from " +
//        "updateComparisonVariantModels(...)")
//  }
//
//  sparkTest("Updated metadata should only differ from old meta data in numSamples" +
//    "and flaggedVariantModels") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val metaData = logisticGnocchiModel.metaData
//    val updatedMetaData = logisticGnocchiModel.updateMetaData(5, List("NewFlaggedVariant"))
//    assert(updatedMetaData.numSamples === metaData.numSamples + 5,
//      "Number of samples in updated metadata incorrect after updateMetaData call.")
//
//    assert(updatedMetaData.modelType === metaData.modelType,
//      "Model type in updated metaData did not match original.")
//
//    assert(updatedMetaData.haplotypeBlockErrorThreshold ===
//      metaData.haplotypeBlockErrorThreshold, "HaplotypeBlockErrorThreshold" +
//      "in updated metaData did not match original.")
//
//    assert(updatedMetaData.phenotype === metaData.phenotype, "Phenotype description " +
//      "in updated metaData did not match original.")
//
//    assert(updatedMetaData.variables ===
//      metaData.variables, "Variables description in updated metaData did not" +
//      "match original.")
//
//    assert(updatedMetaData.flaggedVariantModels ===
//      List("NewFlaggedVariant"), "List of flagged variants in updated metadata" +
//      "incorrect after update.")
//  }
//
//  sparkTest("updateVariantModels should update each logistic variant model") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val updatedlogisticVariantModels = logisticGnocchiModel.updateVariantModels(observationsForUpdate)
//
//    assert(updatedlogisticVariantModels.count === logisticGnocchiModel.variantModels.count(), "Number" +
//      "of variant models after updateVariantModel incorrect")
//
//    val variant0Model = logisticGnocchiModel.variantModels.filter(_.variantId == "variant0").collect.head
//    val updatedVariant0Model = updatedlogisticVariantModels.filter(_.variantId == "variant0").collect.head
//    assert(updatedVariant0Model.weights == variant0Model.update(logisticObsForUpdate0).weights, "Variant model " +
//      "for variant0 incorrect after updateVariantModels call.")
//
//    val variant1Model = logisticGnocchiModel.variantModels.filter(_.variantId == "variant1").collect.head
//    val updatedVariant1Model = updatedlogisticVariantModels.filter(_.variantId == "variant1").collect.head
//    assert(updatedVariant1Model.weights == variant1Model.update(logisticObsForUpdate1).weights, "Variant model " +
//      "for variant1 incorrect after updateVariantModels call.")
//
//    val variant2Model = logisticGnocchiModel.variantModels.filter(_.variantId == "variant2").collect.head
//    val updatedVariant2Model = updatedlogisticVariantModels.filter(_.variantId == "variant2").collect.head
//    assert(updatedVariant2Model.weights == variant2Model.update(logisticObsForUpdate2).weights, "Variant model " +
//      "for variant2 incorrect after updateVariantModels call.")
//  }
//
//  sparkTest("UpdateComparisonModels should return recomputed logistic model and " +
//    "updated data") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val updatedLogisticComparisonVariantModels = logisticGnocchiModel.updateComparisonVariantModels(observationsForUpdate)
//
//    assert(updatedLogisticComparisonVariantModels.count() === logisticGnocchiModel.comparisonVariantModels.count(),
//      "Number of comparison variant models after updatedComparisonModels incorrect.")
//
//    val variant = logisticGnocchiModel.comparisonVariantModels.filter(_._1.variantId == "variant0")
//      .collect.head
//      ._1.variant
//    val comparisonVariantModel = AdditiveLogisticRegression
//      .applyToSite(mergedLogisticDataForComparisonModel, variant, logisticPhenotype, phaseSetId0).toVariantModel
//
//    val (variantModelAfterUpdate, dataAfterUpdate) = updatedLogisticComparisonVariantModels.filter(_._1.variantId == comparisonVariantModel.variantId).collect.head
//    val dataToLists = dataAfterUpdate.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//    assert(variantModelAfterUpdate === comparisonVariantModel,
//      "Comparison variant model for variant0 after updateComparisonModels call" +
//        " does not match expected variant model")
//    val mergedDataForComparisonToLists = mergedLogisticDataForComparisonModel.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//    assert(dataToLists == mergedDataForComparisonToLists,
//      "Merged data in updated comparison model for variant0 did not match expected")
//
//  }
//
//  sparkTest("New gnocchi model after update should have updated meta data") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    // Update logistic model with update group
//    val updatedlogisticGnocchiModel = logisticGnocchiModel.update(observationsForUpdate)
//
//    assert(updatedlogisticGnocchiModel.metaData.numSamples ===
//      logisticGnocchiModel.metaData.numSamples + logisticObsForUpdate0.length,
//      "Number of samples in gnocchi model did not match expected number of samples (10).")
//
//    assert(updatedlogisticGnocchiModel.metaData.modelType === logisticGnocchiModel.metaData.modelType,
//      "Model type of updated model did not match original model type.")
//
//    assert(updatedlogisticGnocchiModel.metaData.haplotypeBlockErrorThreshold ===
//      logisticGnocchiModel.metaData.haplotypeBlockErrorThreshold, "HaplotypeBlockErrorThreshold" +
//      "in updated model did not match original model.")
//
//    assert(updatedlogisticGnocchiModel.metaData.phenotype ===
//      logisticGnocchiModel.metaData.phenotype, "Phenotype description in updated model did not" +
//      "match phenotype description in original model.")
//
//    assert(updatedlogisticGnocchiModel.metaData.variables ===
//      logisticGnocchiModel.metaData.variables, "Variables description in updated model did not" +
//      "match variables description in original model.")
//
//  }
//
//  sparkTest("New logistic gnocchi model after update should have updated variant models") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    val updatedlogisticVariantModels = logisticGnocchiModel.updateVariantModels(observationsForUpdate)
//
//    // Update logistic model with update group
//    val updatedlogisticGnocchiModel = logisticGnocchiModel.update(observationsForUpdate)
//
//    assert(updatedlogisticGnocchiModel.variantModels.collect.head.weights == updatedlogisticVariantModels.collect.head.weights,
//      "Variant Models rdd in updated logistic gnocchi model did not match" +
//        "expected results from updateVariantModels(...)")
//  }
//
//  sparkTest("New logistic gnocchi model after update should have updated comparisonVariantModels") {
//
//    val gmf = gnocchiModelFixture(sc)
//    import gmf._
//
//    // Update linear model with update group
//    val updatedLogisticGnocchiModel = logisticGnocchiModel.update(observationsForUpdate)
//
//    val updateResult = logisticGnocchiModel.updateComparisonVariantModels(observationsForUpdate)
//
//    val (updatedVariantModel, updatedData) = updateResult.filter(_._1.variantId == "variant0").collect.head
//    val updatedDataToLists = updatedData.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//
//    val comparisonModelsAfterUpdate = updatedLogisticGnocchiModel.comparisonVariantModels
//
//    val (variantModelAfterUpdate, dataAfterUpdate) = comparisonModelsAfterUpdate.filter(_._1.variantId == "variant0").collect.head
//    val dataAfterUpdateToLists = dataAfterUpdate.map(pv => {
//      val (p, v) = pv
//      (p, v.toList)
//    }).toList
//
//    assert(dataAfterUpdateToLists == updatedDataToLists,
//      "Merged data in updated comparison model for variant0 in " +
//        "updated linear gnocchi model did not match expected results from " +
//        "updateComparisonVariantModels(...)")
//
//    assert(updatedLogisticGnocchiModel.comparisonVariantModels.count() == logisticGnocchiModel.comparisonVariantModels.count(),
//      "Number of comparison variant models after updatedComparisonModels should match original" +
//        "number of comparison variant models.")
//  }
//
//}