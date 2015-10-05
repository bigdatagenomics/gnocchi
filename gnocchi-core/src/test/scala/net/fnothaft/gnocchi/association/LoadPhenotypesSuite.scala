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
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.avro.Phenotype
import org.bdgenomics.formats.avro.Genotype

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

  var logMsgs = Seq[String]()
  def logFn(s: String) {
    logMsgs = logMsgs :+ s
  }

  sparkTest("validating genotype/phenotype match should succeed if ids match") {
    logMsgs = Seq.empty

    val (mp, mg, samples) = LoadPhenotypes.validateSamples(sc.parallelize(Seq(
      Phenotype.newBuilder()
        .setSampleId("mySample")
        .setPhenotype("myPhenotype")
        .setHasPhenotype(true)
        .build())),
                                                           sc.parallelize(Seq(
                                                             Genotype.newBuilder()
                                                               .setSampleId("mySample")
                                                               .build())),
                                                           logFn)
    assert(mp === 0)
    assert(mg === 0)

    val sArray = samples.collect
    assert(sArray.length === 1)
    assert(sArray.head === "mySample")

    assert(logMsgs.isEmpty)
  }

  sparkTest("validating genotype/phenotype match should fail if ids do not match") {
    logMsgs = Seq.empty

    val (mp, mg, samples) = LoadPhenotypes.validateSamples(sc.parallelize(Seq(
      Phenotype.newBuilder()
        .setSampleId("notMySample")
        .setPhenotype("myPhenotype")
        .setHasPhenotype(true)
        .build())),
                                                           sc.parallelize(Seq(
                                                             Genotype.newBuilder()
                                                               .setSampleId("mySample")
                                                               .build())),
                                                           logFn)
    assert(mp === 1)
    assert(mg === 1)

    val sArray = samples.collect
    assert(sArray.length === 1)
    assert(sArray.head === "mySample")

    assert(logMsgs.size === 4)
    assert(logMsgs(0) === "Missing 1 genotyped sample in phenotypes:")
    assert(logMsgs(1) === "mySample")
    assert(logMsgs(2) === "Missing 1 phenotyped sample in genotypes:")
    assert(logMsgs(3) === "notMySample")
  }

  sparkTest("validating phenotypes should succeed if there is a phenotype per sample") {
    logMsgs = Seq.empty

    val count = LoadPhenotypes.validatePhenotypes(sc.parallelize(Seq(
      Phenotype.newBuilder()
        .setSampleId("mySample")
        .setPhenotype("myPhenotype")
        .setHasPhenotype(true)
        .build())),
      sc.parallelize(Seq("mySample")),
                                                           logFn)
    assert(count === 0)

    assert(logMsgs.isEmpty)
  }

  sparkTest("validating phenotypes should fail if there are missing phenotypes for a sample") {
    logMsgs = Seq.empty

    val count = LoadPhenotypes.validatePhenotypes(sc.parallelize(Seq(
      Phenotype.newBuilder()
        .setSampleId("mySample")
        .setPhenotype("myPhenotype")
        .setHasPhenotype(true)
        .build())),
      sc.parallelize(Seq("mySample", "notMySample")),
                                                           logFn)
    assert(count === 1)

    assert(logMsgs.size === 2)
    assert(logMsgs(0) === "Have 1 missing phenotype observation:")
    assert(logMsgs(1) === "Missing observation of myPhenotype phenotype for sample notMySample.")
  }
}
