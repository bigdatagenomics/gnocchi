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
import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.scalactic.Tolerance._

class LogisticVariantModelSuite extends GnocchiFunSuite {

  sparkTest("Test constructUpdatedVariantModel works") {
    val assoc = LogisticAssociation(geneticParameterStandardError = 0.5,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)

    val variantModel = LogisticVariantModel("rs123456", assoc, "", 1, 1, "A", "C", "")
    val newVariantModel = variantModel.constructUpdatedVariantModel("rs234567", 0.1, 0.2, List(0.3, 0.4), 1)

    // Assert that all values in the LogisticVariantModel object match expected
    // The following values should be updated given the new parameters
    assert(newVariantModel.uniqueID === "rs234567")
    assert(newVariantModel.association.geneticParameterStandardError === 0.1)
    assert(newVariantModel.association.pValue === 0.2)
    assert(newVariantModel.association.weights === List(0.3, 0.4))
    assert(newVariantModel.association.numSamples === 1)

    // The following values should match the original ones
    assert(newVariantModel.phenotype === "")
    assert(newVariantModel.chromosome === 1)
    assert(newVariantModel.position === 1)
    assert(newVariantModel.referenceAllele === "A")
    assert(newVariantModel.alternateAllele === "C")
    assert(newVariantModel.allelicAssumption === "")
  }

  sparkTest("Test constructUpdatedVariantModel with association parameter works") {
    val assoc = LogisticAssociation(geneticParameterStandardError = 0.5,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)

    val variantModel = LogisticVariantModel("rs123456", assoc, "", 1, 1, "A", "C", "")

    val newAssoc = LogisticAssociation(geneticParameterStandardError = 0.1,
      pValue = 0.2,
      weights = List(0.3, 0.4),
      numSamples = 1)

    val newVariantModel = variantModel.constructUpdatedVariantModel("rs234567", newAssoc)

    // Assert that all values in the LogisticVariantModel object match expected
    // The following values should be updated given the new parameters
    assert(newVariantModel.uniqueID === "rs234567")
    assert(newVariantModel.association.geneticParameterStandardError === 0.1)
    assert(newVariantModel.association.pValue === 0.2)
    assert(newVariantModel.association.weights === List(0.3, 0.4))
    assert(newVariantModel.association.numSamples === 1)

    // The following values should match the original ones
    assert(newVariantModel.phenotype === "")
    assert(newVariantModel.chromosome === 1)
    assert(newVariantModel.position === 1)
    assert(newVariantModel.referenceAllele === "A")
    assert(newVariantModel.alternateAllele === "C")
    assert(newVariantModel.allelicAssumption === "")
  }

  sparkTest("Test LogisticVariantModel.mergeWith works") {
    val firstAssoc = LogisticAssociation(geneticParameterStandardError = 0.5,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)
    val firstVariantModel = LogisticVariantModel("rs123456", firstAssoc, "", 1, 1, "A", "C", "")

    val secondAssoc = LogisticAssociation(geneticParameterStandardError = 0.2,
      pValue = 0.2,
      weights = List(0.2, 0.8),
      numSamples = 5)
    val secondVariantModel = LogisticVariantModel("rs123456", secondAssoc, "", 1, 1, "A", "C", "")

    // Merge firstVariantModel with secondVariantModel
    val mergedVariantModel = firstVariantModel.mergeWith(secondVariantModel)

    // Assert values in the merged model match expected
    assert(mergedVariantModel.association.numSamples === 15)

    // Assert that new geneticParameterStandardError is the weighted average of
    // the geneticParameterStandardErrors of the first and second model
    assert(mergedVariantModel.association.geneticParameterStandardError === 0.4)

    // Assert that the new weights are weighted averages of the weights from
    // the first and second model
    assert(mergedVariantModel.association.weights === List(0.4, 0.6))

    // The new p-value should be 0.1336...
    assert(mergedVariantModel.association.pValue === 0.1336 +- 0.0001)
  }
}
