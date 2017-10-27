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

import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.primitives.association.LinearAssociation
import org.scalactic.Tolerance._

class LinearVariantModelSuite extends GnocchiFunSuite {

  sparkTest("Test constructUpdatedVariantModel works") {
    val assoc = LinearAssociation(ssDeviations = 0.5,
      ssResiduals = 0.5,
      geneticParameterStandardError = 0.5,
      tStatistic = 0.5,
      residualDegreesOfFreedom = 2,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)

    val variantModel = LinearVariantModel("rs123456", assoc, "", 1, 1, "A", "C", "")

    val newVariantModel = variantModel.constructUpdatedVariantModel("rs234567",
      0.1,
      0.2,
      0.3,
      0.4,
      100,
      0.6,
      List(0.7, 0.8),
      500)

    // Assert that all values in the LinearVariantModel object match expected
    // The following values should be updated given the new parameters
    assert(newVariantModel.uniqueID === "rs234567")
    assert(newVariantModel.association.ssDeviations === 0.1)
    assert(newVariantModel.association.ssResiduals === 0.2)
    assert(newVariantModel.association.geneticParameterStandardError === 0.3)
    assert(newVariantModel.association.tStatistic === 0.4)
    assert(newVariantModel.association.residualDegreesOfFreedom === 100)
    assert(newVariantModel.association.pValue === 0.6)
    assert(newVariantModel.association.weights === List(0.7, 0.8))
    assert(newVariantModel.association.numSamples === 500)

    // The following values should match the original ones
    assert(newVariantModel.phenotype === "")
    assert(newVariantModel.chromosome === 1)
    assert(newVariantModel.position === 1)
    assert(newVariantModel.referenceAllele === "A")
    assert(newVariantModel.alternateAllele === "C")
    assert(newVariantModel.allelicAssumption === "")
  }

  sparkTest("Test constructUpdatedVariantModel with association parameter works") {
    val assoc = LinearAssociation(ssDeviations = 0.5,
      ssResiduals = 0.5,
      geneticParameterStandardError = 0.5,
      tStatistic = 0.5,
      residualDegreesOfFreedom = 2,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)

    val variantModel = LinearVariantModel("rs123456", assoc, "", 1, 1, "A", "C", "")

    val newAssoc = LinearAssociation(ssDeviations = 0.1,
      ssResiduals = 0.2,
      geneticParameterStandardError = 0.3,
      tStatistic = 0.4,
      residualDegreesOfFreedom = 100,
      pValue = 0.6,
      weights = List(0.7, 0.8),
      numSamples = 500)

    val newVariantModel = variantModel.constructUpdatedVariantModel("rs234567", newAssoc)

    // Assert that all values in the LinearVariantModel object match expected
    // The following values should be updated given the new parameters
    assert(newVariantModel.uniqueID === "rs234567")
    assert(newVariantModel.association.ssDeviations === 0.1)
    assert(newVariantModel.association.ssResiduals === 0.2)
    assert(newVariantModel.association.geneticParameterStandardError === 0.3)
    assert(newVariantModel.association.tStatistic === 0.4)
    assert(newVariantModel.association.residualDegreesOfFreedom === 100)
    assert(newVariantModel.association.pValue === 0.6)
    assert(newVariantModel.association.weights === List(0.7, 0.8))
    assert(newVariantModel.association.numSamples === 500)

    // The following values should match the original ones
    assert(newVariantModel.phenotype === "")
    assert(newVariantModel.chromosome === 1)
    assert(newVariantModel.position === 1)
    assert(newVariantModel.referenceAllele === "A")
    assert(newVariantModel.alternateAllele === "C")
    assert(newVariantModel.allelicAssumption === "")
  }

  sparkTest("Test LinearVariantModel.mergeWith works") {
    val firstAssoc = LinearAssociation(ssDeviations = 0.5,
      ssResiduals = 0.5,
      geneticParameterStandardError = 0.5,
      tStatistic = 0.5,
      residualDegreesOfFreedom = 2,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)

    val firstVariantModel = LinearVariantModel("rs123456", firstAssoc, "", 1, 1, "A", "C", "")

    val secondAssoc = LinearAssociation(ssDeviations = 0.2,
      ssResiduals = 0.3,
      geneticParameterStandardError = 0.4,
      tStatistic = 0.6,
      residualDegreesOfFreedom = 1,
      pValue = 0.6,
      weights = List(0.7, 0.8),
      numSamples = 10)

    val secondVariantModel = LinearVariantModel("rs123456", secondAssoc, "", 1, 1, "A", "C", "")

    // Merge firstVariantModel with secondVariantModel
    val mergedVariantModel = firstVariantModel.mergeWith(secondVariantModel)

    assert(mergedVariantModel.association.numSamples === 20)
    assert(mergedVariantModel.association.weights === List(0.6, 0.65))
    assert(mergedVariantModel.association.ssDeviations === 0.7)
    assert(mergedVariantModel.association.ssResiduals === 0.8)
    assert(mergedVariantModel.association.geneticParameterStandardError === 0.2519 +- 0.0001)
    assert(mergedVariantModel.association.residualDegreesOfFreedom === 12)
    assert(mergedVariantModel.association.tStatistic === 2.5796 +- 0.0001)
    assert(mergedVariantModel.association.pValue === 0.0241 +- 0.0001)
  }

}
