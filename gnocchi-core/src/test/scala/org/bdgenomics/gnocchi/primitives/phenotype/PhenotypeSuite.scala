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
package org.bdgenomics.gnocchi.primitives.phenotype

import org.bdgenomics.gnocchi.utils.GnocchiFunSuite

class PhenotypeSuite extends GnocchiFunSuite {
  // toList tests
  sparkTest("Phenotype.toList should return the list of phenotypes stored inside.") {
    val pheno = Phenotype(sampleId = "1234",
      phenoName = "testPheno",
      phenotype = 12.34,
      covariates = List(12, 13, 14))

    assert(pheno.toList == List[Double](12.34, 12.0, 13.0, 14.0), "Phenotype.toList does correctly convert a phenotype object's data to a list.")
  }

  sparkTest("Phenotype constructor should be able to create a Phenotype object without covariates.") {
    val pheno = Phenotype(sampleId = "1234",
      phenoName = "testPheno",
      phenotype = 12.34,
      covariates = List())

    assert(pheno.covariates == List[Double](), "Phenotype.toList does correctly convert a phenotype object's data to a list.")
  }
}
