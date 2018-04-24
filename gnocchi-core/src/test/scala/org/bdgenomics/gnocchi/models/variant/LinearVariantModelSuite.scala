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
package org.bdgenomics.gnocchi.models.variant

import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.scalactic.Tolerance._

class LinearVariantModelSuite extends GnocchiFunSuite {

  sparkTest("LinearVariantModel.createAssociation[Additive] should correctly construct and association.") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        "7677" -> GenotypeState(1, 1, 0),
        "5218" -> GenotypeState(2, 0, 0),
        "1939" -> GenotypeState(0, 2, 0),
        "5695" -> GenotypeState(1, 1, 0),
        "4626" -> GenotypeState(2, 0, 0),
        "1933" -> GenotypeState(1, 1, 0),
        "1076" -> GenotypeState(2, 0, 0),
        "1534" -> GenotypeState(0, 2, 0),
        "1615" -> GenotypeState(2, 0, 0)))

    val phenotypes =
      Map(
        "1939" -> Phenotype("1939", "pheno_1", 51.75673646004061, List()),
        "2792" -> Phenotype("2792", "pheno_1", 62.22934974654722, List()),
        "1534" -> Phenotype("1534", "pheno_1", 51.568591214841405, List()),
        "5218" -> Phenotype("5218", "pheno_1", 51.57128192897128, List()),
        "4626" -> Phenotype("4626", "pheno_1", 71.50143228329485, List()),
        "1933" -> Phenotype("1933", "pheno_1", 61.24800827007204, List()),
        "5695" -> Phenotype("5695", "pheno_1", 56.08340448063026, List()),
        "1076" -> Phenotype("1076", "pheno_1", 63.756223063707154, List()),
        "1615" -> Phenotype("1615", "pheno_1", 69.45757502327798, List()),
        "7677" -> Phenotype("7677", "pheno_1", 60.18207070928484, List()))

    val model1 = LinearVariantModel("rs8330247",
      14,
      21373362,
      "C",
      "T",
      9,
      2,
      Array(9.0, 7.0, 7.0, 11.0),
      Array(537.1253234341203, 384.1641388097512),
      7,
      List(64.3845917221413, -6.0480002950216205))

    val correctAssoc = LinearAssociation("rs8330247",
      14,
      21373362,
      9,
      0.0514786205327481,
      2.5793052054973695,
      258.7205966763378,
      -2.3448176207031612)

    val returnedAssoc = model1.createAssociation(variant, phenotypes, "ADDITIVE")

    assert(correctAssoc.uniqueID == returnedAssoc.uniqueID, "Incorrect association!")
    assert(correctAssoc.chromosome == returnedAssoc.chromosome, "Incorrect association!")
    assert(correctAssoc.position == returnedAssoc.position, "Incorrect association!")
    assert(correctAssoc.numSamples == returnedAssoc.numSamples, "Incorrect association!")
    assert(correctAssoc.pValue === returnedAssoc.pValue +- 0.00005, "Incorrect association!")
    assert(correctAssoc.genotypeStandardError === returnedAssoc.genotypeStandardError +- 0.00005, "Incorrect association!")
    assert(correctAssoc.ssResiduals === returnedAssoc.ssResiduals +- 0.00005, "Incorrect association!")
    assert(correctAssoc.tStatistic === returnedAssoc.tStatistic +- 0.00005, "Incorrect association!")
  }

  sparkTest("LinearVariantModel.createAssociation[Dominant] should correctly construct an association.") {
    val variant = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        "7677" -> GenotypeState(1, 1, 0),
        "5218" -> GenotypeState(2, 0, 0),
        "1939" -> GenotypeState(0, 2, 0),
        "5695" -> GenotypeState(1, 1, 0),
        "4626" -> GenotypeState(2, 0, 0),
        "1933" -> GenotypeState(1, 1, 0),
        "1076" -> GenotypeState(2, 0, 0),
        "1534" -> GenotypeState(0, 2, 0),
        "1615" -> GenotypeState(2, 0, 0)))

    val phenotypes =
      Map(
        "1939" -> Phenotype("1939", "pheno_1", 51.75673646004061, List()),
        "2792" -> Phenotype("2792", "pheno_1", 62.22934974654722, List()),
        "1534" -> Phenotype("1534", "pheno_1", 51.568591214841405, List()),
        "5218" -> Phenotype("5218", "pheno_1", 51.57128192897128, List()),
        "4626" -> Phenotype("4626", "pheno_1", 71.50143228329485, List()),
        "1933" -> Phenotype("1933", "pheno_1", 61.24800827007204, List()),
        "5695" -> Phenotype("5695", "pheno_1", 56.08340448063026, List()),
        "1076" -> Phenotype("1076", "pheno_1", 63.756223063707154, List()),
        "1615" -> Phenotype("1615", "pheno_1", 69.45757502327798, List()),
        "7677" -> Phenotype("7677", "pheno_1", 60.18207070928484, List()))

    val model1 = LinearVariantModel("rs8330247",
      14,
      21373362,
      "C",
      "T",
      9,
      2,
      Array(9.0, 5.0, 5.0, 5.0),
      Array(537.1253234341203, 280.83881113486916),
      7,
      List(64.07162807481279, -7.9038658478389605))

    val correctAssoc = LinearAssociation("rs8330247",
      14,
      21373362,
      9,
      0.12646717194634544,
      4.557551693699016,
      323.10876018926973,
      -1.7342350408809073)

    val returnedAssoc = model1.createAssociation(variant, phenotypes, "DOMINANT")

    assert(correctAssoc.uniqueID == returnedAssoc.uniqueID, "Incorrect association!")
    assert(correctAssoc.chromosome == returnedAssoc.chromosome, "Incorrect association!")
    assert(correctAssoc.position == returnedAssoc.position, "Incorrect association!")
    assert(correctAssoc.numSamples == returnedAssoc.numSamples, "Incorrect association!")
    assert(correctAssoc.pValue === returnedAssoc.pValue +- 0.00005, "Incorrect association!")
    assert(correctAssoc.genotypeStandardError === returnedAssoc.genotypeStandardError +- 0.00005, "Incorrect association!")
    assert(correctAssoc.ssResiduals === returnedAssoc.ssResiduals +- 0.00005, "Incorrect association!")
    assert(correctAssoc.tStatistic === returnedAssoc.tStatistic +- 0.00005, "Incorrect association!")

  }

  sparkTest("LinearVariantModel.mergeWith correctly merges together two LinearVariantModels") {
    val model1 = LinearVariantModel("rs1085962",
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

    val model2 = LinearVariantModel("rs1085962",
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

    val mergedByHand = LinearVariantModel("rs1085962",
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

    val mergedTest = model1.mergeWith(model2)
    assert(mergedByHand.uniqueID == mergedTest.uniqueID, "Merging variant model is incorrect for uniqueID")
    assert(mergedByHand.chromosome == mergedTest.chromosome, "Merging variant model is incorrect for chromosome")
    assert(mergedByHand.position == mergedTest.position, "Merging variant model is incorrect for position")
    assert(mergedByHand.referenceAllele == mergedTest.referenceAllele, "Merging variant model is incorrect for referenceAllele")
    assert(mergedByHand.alternateAllele == mergedTest.alternateAllele, "Merging variant model is incorrect for alternateAllele")
    assert(mergedByHand.numSamples == mergedTest.numSamples, "Merging variant model is incorrect for numSamples")
    assert(mergedByHand.numPredictors == mergedTest.numPredictors, "Merging variant model is incorrect for numPredictors")
    assert(mergedByHand.xTx.toList == mergedTest.xTx.toList, "Merging variant model is incorrect for xTx")
    assert(mergedByHand.xTy.toList == mergedTest.xTy.toList, "Merging variant model is incorrect for xTy")
    assert(mergedByHand.residualDegreesOfFreedom == mergedTest.residualDegreesOfFreedom, "Merging variant model is incorrect for residualDegreesOfFreedom")
    assert(mergedByHand.weights == mergedTest.weights, "Merging variant model is incorrect for weights")
  }
}
