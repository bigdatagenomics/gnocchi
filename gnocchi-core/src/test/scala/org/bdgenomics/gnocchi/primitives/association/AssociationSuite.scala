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
package org.bdgenomics.gnocchi.primitives.association

import org.bdgenomics.gnocchi.GnocchiFunSuite

class AssociationSuite extends GnocchiFunSuite {
  sparkTest("LinearAssociation creation works.") {
    val assoc = LinearAssociation(ssDeviations = 0.5,
      ssResiduals = 0.5,
      geneticParameterStandardError = 0.5,
      tStatistic = 0.5,
      residualDegreesOfFreedom = 2,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)
    assert(assoc.isInstanceOf[LinearAssociation], "Cannot create LinearAssociation")
  }

  sparkTest("LogisticAssociation creation works.") {
    val assoc = LogisticAssociation(geneticParameterStandardError = 0.5,
      pValue = 0.5,
      weights = List(0.5, 0.5),
      numSamples = 10)
    assert(assoc.isInstanceOf[LogisticAssociation], "Cannot create LogisticAssociation")
  }
}
