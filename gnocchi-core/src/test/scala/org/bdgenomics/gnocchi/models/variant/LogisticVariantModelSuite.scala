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

import org.bdgenomics.gnocchi.primitives.association.LogisticAssociation
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.scalactic.Tolerance._

class LogisticVariantModelSuite extends GnocchiFunSuite {

  sparkTest("Test constructUpdatedVariantModel works") {
    val assoc = LogisticAssociation(
      "rs123456",
      1,
      1,
      numSamples = 10,
      pValue = 0.5,
      genotypeStandardError = 0.5)

    val variantModel = LogisticVariantModel("rs123456", 1, 1, "A", "C", List(5.0, 4.0, 3.0))

    // The following values should match the original ones
    assert(variantModel.uniqueID === "rs123456")
    assert(variantModel.chromosome === 1)
    assert(variantModel.position === 1)
    assert(variantModel.referenceAllele === "A")
    assert(variantModel.alternateAllele === "C")
    assert(variantModel.weights === List(5.0, 4.0, 3.0))
  }
}
