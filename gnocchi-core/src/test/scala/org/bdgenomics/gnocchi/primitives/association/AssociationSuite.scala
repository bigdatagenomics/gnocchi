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

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite

class AssociationSuite extends GnocchiFunSuite {
  sparkTest("LinearAssociation creation works.") {
    val uniqueID = "rs123456"
    val chromosome = 10
    val position = 12345667
    val numSamples = 23
    val genotypeStandardError = 5.12
    val pValue = 0.001
    val ssResiduals = 1234.21
    val tStatistic = 2.123

    val assoc = LinearAssociation(
      uniqueID,
      chromosome,
      position,
      numSamples,
      pValue,
      genotypeStandardError,
      ssResiduals,
      tStatistic)

    assert(assoc.isInstanceOf[LinearAssociation], "Cannot create LinearAssociation")
  }

  sparkTest("LogisticAssociation creation works.") {
    val uniqueID = "rs123456"
    val chromosome = 10
    val position = 12345667
    val numSamples = 23
    val genotypeStandardError = 5.12
    val pValue = 0.001
    val ssResiduals = 1234.21
    val tStatistic = 2.123

    val assoc = LogisticAssociation(
      uniqueID,
      chromosome,
      position,
      numSamples,
      pValue,
      genotypeStandardError)

    assert(assoc.isInstanceOf[LogisticAssociation], "Cannot create LogisticAssociation")
  }
}
