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
package org.bdgenomics.gnocchi.sql

import org.bdgenomics.gnocchi.utils.GnocchiFunSuite
import org.bdgenomics.gnocchi.sql.GnocchiSession._

class GenotypeDatasetSuite extends GnocchiFunSuite {
  sparkTest("GenotypeDataset.save should save a GenotypeDataset object for future use.") {
    val testGenos = testFile("time_genos_1.vcf")
    val genotypes = sc.loadGenotypes(testGenos, "", "ADDITIVE")

    val tempFile = tmpFile("out/")

    try {
      genotypes.save(tempFile)
    } catch {
      case e: Throwable => { print(e); fail("Error on save gnocchi formatted genotypes") }
    }

    val loadedGenotypes = try {
      sc.loadGnocchiGenotypes(tempFile)
    } catch {
      case e: Throwable => { print(e); fail("Error on load gnocchi formatted genotypes") }
    }

    assert(loadedGenotypes.allelicAssumption == genotypes.allelicAssumption, "Allelic assumption changed!")
    assert(loadedGenotypes.datasetUID == genotypes.datasetUID, "The Dataset Unique ID changed!")
    assert(loadedGenotypes.sampleUIDs == genotypes.sampleUIDs, "the sampleUIDs changed!")
  }

  sparkTest("GenotypeDataset.transformAllelicAssumption should change the allelic assumption and nothing else.") {
    val testGenos = testFile("time_genos_1.vcf")
    val genotypes = sc.loadGenotypes(testGenos, "", "ADDITIVE")

    val recodedGenotypes = genotypes.transformAllelicAssumption("DOMINANT")

    assert(recodedGenotypes.allelicAssumption == "DOMINANT", "Changing allelic assumption fails")
    assert(recodedGenotypes.datasetUID == genotypes.datasetUID, "The Dataset Unique ID changed!")
    assert(recodedGenotypes.sampleUIDs == genotypes.sampleUIDs, "the sampleUIDs changed!")
  }
}
