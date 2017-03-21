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
package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.{ GenotypeState, Phenotype }

class GnocchiModelSuite extends GnocchiFunSuite {

  /* initialGroupGenotypes 3 variants, 5 samples

  #CHROM	POS	    ID	        REF	ALT	QUAL	FILTER	INFO	FORMAT	sample1	sample2	sample3	sample4	sample5
  1	      752721	rs3131971	  A	  G	  60	  PASS	  .	    GT	    0/0	    0/0	    0/0	    0/1	    1/0
  1	      752722	rs3131972	  A	  T	  60	  PASS	  .	    GT	    1/1	    1/1	    1/1	    1/1	    0/1
  1	      752723	rs3131973	  C	  G	  60	  PASS	  .	    GT	    0/1	    0/0	    0/0	    1/1	    1/0

  */

  // GenotypeState objects for rs3131971
  val gs1 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample1", 0, 0)
  val gs2 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample2", 0, 0)
  val gs3 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample3", 0, 0)
  val gs4 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample4", 1, 0)
  val gs5 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample5", 1, 0)

  // GenotypeState objects for rs3131972
  val gs6 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample1", 2, 0)
  val gs7 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample2", 2, 0)
  val gs8 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample3", 2, 0)
  val gs9 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample4", 2, 0)
  val gs10 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample5", 1, 0)

  // GenotypeState objects for rs3131972
  val gs11 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample1", 1, 0)
  val gs12 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample2", 0, 0)
  val gs13 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample3", 0, 0)
  val gs14 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample4", 2, 0)
  val gs15 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample5", 1, 0)

  // Phenotypes for first 5 samples
  val p1 = Phenotype("phenotype", "sample1", Array(1.4, 0.8404, 2.9080))
  val p2 = Phenotype("phenotype", "sample2", Array(0.1, -0.8880, 0.8252))
  val p3 = Phenotype("phenotype", "sample3", Array(2.3, 0.1001, 1.3790))
  val p4 = Phenotype("phenotype", "sample4", Array(6.0, -0.5445, -1.0582))
  val p5 = Phenotype("phenotype", "sample5", Array(1.1, 0.3035, -0.4686))

  // package initialGroupGenotypes
  val initialGroupGenotypes = sc.parallelize(List(gs1, gs2, gs3, gs4, gs5, gs6, gs7, gs8,
    gs9, gs10, gs11, gs12, gs13, gs14, gs15))

  // package initialGroupPhenotypes
  val initialGroupPhenotypes = sc.parallelize(List(p1, p2, p3, p4, p5))

  // Build linear GnocchiModel with 3 variants on initial group
  val (linearGnocchiModel, associationsFromLinear) = BuildAdditiveLinearGnocchiModel(initialGroupGenotypes, initialGroupPhenotypes, sc)

  /* updateGroupGenotypes 3 variants, 5 samples
  #CHROM	POS	    ID	        REF	ALT	QUAL	FILTER	INFO	FORMAT	sample6	sample7	sample8	sample9	sample10
  1	      752721	rs3131971	  A	  G	  60	  PASS	  .	    GT	    1/0	    1/1	    1/1	    1/1	    1/1
  1	      752722	rs3131972	  A	  T	  60	  PASS	  .	    GT	    1/0	    1/0	    0/0	    0/0	    0/0
  1	      752723	rs3131973	  C	  G	  60	  PASS	  .	    GT	    1/0	    1/1	    0/0	    1/1	    1/1
   */

  // GenotypeState objects for rs3131971
  val gs16 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample6", 1, 0)
  val gs17 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample7", 2, 0)
  val gs18 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample8", 2, 0)
  val gs19 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample9", 2, 0)
  val gs20 = GenotypeState("ShouldBeFlagged", 752721, 752722, "A", "G", "sample10", 2, 0)

  // GenotypeState objects for rs3131972
  val gs21 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample6", 1, 0)
  val gs22 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample7", 1, 0)
  val gs23 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample8", 0, 0)
  val gs24 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample9", 0, 0)
  val gs25 = GenotypeState("ShouldNotBeFlagged", 752722, 752723, "A", "T", "sample10", 0, 0)

  // GenotypeState objects for rs3131972
  val gs26 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample6", 1, 0)
  val gs27 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample7", 2, 0)
  val gs28 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample8", 0, 0)
  val gs29 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample9", 2, 0)
  val gs30 = GenotypeState("ShouldNotBeFlagged2", 752723, 752724, "C", "G", "sample10", 2, 0)

  // Phenotypes for last 5 samples
  val p6 = Phenotype("phenotype", "sample6", Array(2, -0.6003, -0.2725))
  val p7 = Phenotype("phenotype", "sample7", Array(2, 0.4900, 1.0984))
  val p8 = Phenotype("phenotype", "sample8", Array(3, 0.7394, -0.2779))
  val p9 = Phenotype("phenotype", "sample9", Array(3, 1.7119, 0.7015))
  val p10 = Phenotype("phenotype", "sample10", Array(3, -0.1941, -2.0518))

  // package initialGroupGenotypes
  val updateGroupGenotypes = sc.parallelize(List(gs16, gs17, gs18, gs19, gs20, gs21, gs22, gs23,
    gs24, gs25, gs26, gs27, gs28, gs29, gs30))

  // package initialGroupPhenotypes
  val updateGroupPhenotypes = sc.parallelize(List(p6, p7, p8, p9, p10))

  // Update linear model with update group
  linearGnocchiModel.update(updateGroupGenotypes, updateGroupPhenotypes, sc)

  ignore("Check that updated number of samples in linear model is correct") {
    //TODO: Determine the correct value for numSamples
    linearGnocchiModel.metaData.numSamples = 0
  }

  //TODO: figure out now Haplytype blocks must be encoded in Gnocchi
  val variantModelsInHaplotypeBlock = linearGnocchiModel.getVariantsFromHaplotypeBlock("BlockShouldBeFlagged").collect.toList

  ignore("Check output of getVariantsFromHaplotypeBlock") {
    val variantModel = variantModelsInHaplotypeBlock.head
    assert(variantModel.variantId == "VariantShouldBeFlagged")
  }

  //TODO: implement this test
  ignore("Check that correct variant gets flagged for re-compute")

  ignore("Check that flagged incremental linear model has correct weights") {
    //TODO: calculate what the correct weights should be
    val w: Array[Double] = linearGnocchiModel.getVariantsFromHaplotypeBlock("BlockShouldBeFlagged")
      .take(1)(0).weights
    assert(w sameElements Array(0, 0, 0, 0))
  }

  ignore("Check that comparison linear model has correct weights and updated data has correct length") {
    //TODO: calculate the correct linear weights and the correct number of observations
    val (model, observations) = linearGnocchiModel.comparisonVariantModels.take(1)(0)
    assert(model.weights sameElements Array(0, 0, 0, 0))
    assert(observations.length == 0)
  }

  ignore("check that correct linear VariantModel gets flagged") {
    assert(linearGnocchiModel.metaData.flaggedVariantModels.head == "ShouldBeFlagged")
  }

  // Phenotypes for first 5 samples of logistic model
  val lp1 = Phenotype("phenotype", "sample1", Array(1, 0.8404, 2.9080))
  val lp2 = Phenotype("phenotype", "sample2", Array(0, -0.8880, 0.8252))
  val lp3 = Phenotype("phenotype", "sample3", Array(0, 0.1001, 1.3790))
  val lp4 = Phenotype("phenotype", "sample4", Array(1, -0.5445, -1.0582))
  val lp5 = Phenotype("phenotype", "sample5", Array(1, 0.3035, -0.4686))

  // package initialGroupPhenotypes
  val initialGroupBinaryPhenotypes = sc.parallelize(List(lp1, lp2, lp3, lp4, lp5))

  // Build linear GnocchiModel with 3 variants on initial group
  val (logisticGnocchiModel, associationsFromLogistic) = BuildAdditiveLogisticGnocchiModel(initialGroupGenotypes, initialGroupBinaryPhenotypes, sc)

  // Phenotypes for last 5 samples
  val lp6 = Phenotype("phenotype", "sample6", Array(1, -0.6003, -0.2725))
  val lp7 = Phenotype("phenotype", "sample7", Array(1, 0.4900, 1.0984))
  val lp8 = Phenotype("phenotype", "sample8", Array(0, 0.7394, -0.2779))
  val lp9 = Phenotype("phenotype", "sample9", Array(0, 1.7119, 0.7015))
  val lp10 = Phenotype("phenotype", "sample10", Array(0, -0.1941, -2.0518))

  // package initialGroupPhenotypes
  val updateGroupBinaryPhenotypes = sc.parallelize(List(lp6, lp7, lp8, lp9, lp10))

  // Update logistic model with update group
  logisticGnocchiModel.update(updateGroupGenotypes, updateGroupBinaryPhenotypes, sc)

  ignore("Check that updated number of samples in logistic model is correct") {
    //TODO: Determine the correct value for numSamples
    assert(logisticGnocchiModel.metaData.numSamples == 0)
  }

  //TODO: figure out now Haplytype blocks must be encoded in Gnocchi
  val logisticVariantModelsInHaplotypeBlock = logisticGnocchiModel.getVariantsFromHaplotypeBlock("ShouldBeFlagged").collect.toList
  ignore("Check output of getVariantsFromHaplotypeBlock") {
    val variantModel = logisticVariantModelsInHaplotypeBlock.head
    assert(variantModel.variantId == "Specified ID")
  }

  ignore("Check that incremental logistic model for comparison has correct weights") {
    //TODO: calculate what the correct weights should be
    val w: Array[Double] = logisticGnocchiModel.getComparisonVariantsFromHaplotypeBlock("ShouldBeFlagged").take(1).head.weights
    assert(w sameElements Array(0, 0, 0, 0))
  }

  ignore("Check that comparison linear model has correct weights and updated data has correct length") {
    //TODO: calculate the correct logistic weights and the correct number of observations
    val (model, observations) = logisticGnocchiModel.comparisonVariantModels.take(1).head
    assert(model.weights sameElements Array(0, 0, 0, 0))
    assert(observations.length == 0)
  }

  ignore("check that correct logistic VariantModel gets flagged") {
    assert(logisticGnocchiModel.metaData.flaggedVariantModels.head == "ShouldBeFlagged")
  }

}