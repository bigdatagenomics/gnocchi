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

package org.bdgenomics.gnocchi.api

import org.bdgenomics.gnocchi.api.java.GnocchiFunSuite
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.{ GenotypeDataset, GnocchiSession }
import org.apache.spark.SparkContext
import org.mockito.Mockito
import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.api.java.core.JavaGnocchiSession
import org.scalatest.FunSuite // (TODO) Replace with GnocchiFunSuite

class JavaGnocchiSessionSuite extends GnocchiFunSuite {
  var gs: GnocchiSession = null
  var jgs: JavaGnocchiSession = null

  sparkBefore("Creating JavaGnocchiSession") {
    gs = Mockito.mock(classOf[GnocchiSession])
    jgs = new JavaGnocchiSession(gs)
  }

  ignore("Verify filterSamples makes correct call to GnocchiSession") {
    val mockGenotype = Mockito.mock(classOf[GenotypeDataset])
    val mockMind = 0.0
    val mockPloidy = 0.0

    jgs.filterSamples(mockGenotype, mockMind, mockPloidy)

    Mockito.verify(gs).filterSamples(mockGenotype, mockMind, mockPloidy)
  }

  ignore("Verify filterVariants makes correct call to GnocchiSession") {
    val mockGenotype = Mockito.mock(classOf[Dataset[CalledVariant]])
    val mockGeno = 0.0
    val mockMaf = 0.0

    jgs.filterVariants(mockGenotype, mockGeno, mockMaf)

    Mockito.verify(gs).filterVariants(mockGenotype, mockGeno, mockMaf)
  }

  ignore("Verify recodeMajorAllele makes correct call to Gnocchi Sesssion") {
    val mockGenotype = Mockito.mock(classOf[Dataset[CalledVariant]])

    jgs.recodeMajorAllele(mockGenotype)

    Mockito.verify(gs).recodeMajorAllele(mockGenotype)
  }

  ignore("Verify loadGenotypes makes correct call to Gnocchi Sesssion") {
    val mockGeno = ""
    val mockDatasetName = ""
    val mockAllelicAssumption = ""

    jgs.loadGenotypes(mockGeno, mockDatasetName, mockAllelicAssumption)

    Mockito.verify(gs).loadGenotypes(mockGeno, mockDatasetName, mockAllelicAssumption)
  }

  ignore("Verify loadPhenotypes makes correct call to Gnocchi Sesssion") {
    val mockPhenotypesPath = ""
    val mockPrimaryID = ""
    val mockPhenoName = ""
    val mockDelimiter = ""

    jgs.loadPhenotypes(mockPhenotypesPath, mockPrimaryID, mockPhenoName, mockDelimiter, null, null)

    Mockito.verify(gs).loadPhenotypes(mockPhenotypesPath, mockPrimaryID, mockPhenoName, mockDelimiter, None, None, "\t", List("-9"))
  }
}
