/**
 * Copyright 2015 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi

import net.fnothaft.gnocchi.avro.Phenotype

class LoadPhenotypeSuite extends GnocchiFunSuite {

  test("load simple phenotypes") {
    val p1 = LoadPhenotypes.parseLine("mySample, a phenotype, true")
    assert(p1.getSampleId === "mySample")
    assert(p1.getPhenotype === "a phenotype")
    assert(p1.getHasPhenotype)
    
    val p2 = LoadPhenotypes.parseLine("mySample, another phenotype, false")
    assert(p2.getSampleId === "mySample")
    assert(p2.getPhenotype === "another phenotype")
    assert(!p2.getHasPhenotype)

    intercept[AssertionError] {
      LoadPhenotypes.parseLine("mySample, bad phenotype")
    }
  }

  sparkTest("load phenotypes from a file") {
    val filepath = ClassLoader.getSystemClassLoader.getResource("samplePhenotypes.csv").getFile
    
    val phenotypes = LoadPhenotypes(filepath, sc)
      .collect()
    
    assert(phenotypes.length === 2)
    assert(phenotypes.forall(_.getSampleId == "mySample"))
    assert(phenotypes.filter(_.getPhenotype == "a phenotype").length === 1)
    assert(phenotypes.filter(_.getPhenotype == "a phenotype").head.getHasPhenotype)
    assert(phenotypes.filter(_.getPhenotype == "another phenotype").length === 1)
    assert(!phenotypes.filter(_.getPhenotype == "another phenotype").head.getHasPhenotype)
  }
}
