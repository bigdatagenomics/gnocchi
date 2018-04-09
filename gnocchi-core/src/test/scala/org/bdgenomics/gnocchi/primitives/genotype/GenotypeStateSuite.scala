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
package org.bdgenomics.gnocchi.primitives.genotype

import org.bdgenomics.gnocchi.utils.GnocchiFunSuite

class GenotypeStateSuite extends GnocchiFunSuite {

  // to Double tests
  sparkTest("GenotypeState.toDouble should exit gracefully when there are no valid calls.") {
    val gs = GenotypeState("1234", 0, 0, 2)
    assert(gs.toDouble == 0.0, "GenotypeState.toDouble does not deal with all missing values correctly.")
  }

  sparkTest("GenotypeState.toDouble should count the number of matches to the alternate allele.") {
    val gs = GenotypeState("1234", 1, 1, 0)
    assert(gs.toDouble == 1.0, "GenotypeState.toDouble does not correctly count the number of alternate alleles.")
  }

  sparkTest("GenotypeState.toDouble should count the number of matches to the alternate allele when there are missing values.") {
    val gs = GenotypeState("1234", 0, 1, 1)
    assert(gs.toDouble == 1.0, "GenotypeState.toDouble does not correctly count the number of alternate alleles when there are missing values.")
  }

  // Allelic Assumption tests

  sparkTest("GenotypeState.dominant should map 0.0 to 0.0 and everything else to 1.0") {
    val gs0 = GenotypeState("0", 2, 0, 0)
    val gs1 = GenotypeState("1", 1, 1, 0)
    val gs2 = GenotypeState("2", 0, 2, 0)

    assert(gs2.dominant == 1.0,
      "GenotypeState.dominant does not correctly map 2.0 to 1.0")
    assert(gs1.dominant == 1.0,
      "GenotypeState.dominant does not correctly map 1.0 to 1.0")
    assert(gs0.dominant == 0.0,
      "GenotypeState.dominant does not correctly map 0.0 to 0.0")
  }

  sparkTest("GenotypeState.additive should be an identity map") {
    val gs0 = GenotypeState("0", 2, 0, 0)
    val gs1 = GenotypeState("1", 1, 1, 0)
    val gs2 = GenotypeState("2", 0, 2, 0)

    assert(gs2.additive == 2.0,
      "GenotypeState.additive does not correctly map 2.0 to 2.0")
    assert(gs1.additive == 1.0,
      "GenotypeState.additive does not correctly map 1.0 to 1.0")
    assert(gs0.additive == 0.0,
      "GenotypeState.additive does not correctly map 0.0 to 0.0")
  }

  sparkTest("GenotypeState.recessive should map 2.0 to 1.0 and everything else to 0.0") {
    val gs0 = GenotypeState("0", 2, 0, 0)
    val gs1 = GenotypeState("1", 1, 1, 0)
    val gs2 = GenotypeState("2", 0, 2, 0)

    assert(gs2.recessive == 1.0,
      "GenotypeState.recessive does not correctly map 2.0 to 1.0")
    assert(gs1.recessive == 0.0,
      "GenotypeState.recessive does not correctly map 1.0 to 0.0")
    assert(gs0.recessive == 0.0,
      "GenotypeState.recessive does not correctly map 0.0 to 0.0")
  }
}
