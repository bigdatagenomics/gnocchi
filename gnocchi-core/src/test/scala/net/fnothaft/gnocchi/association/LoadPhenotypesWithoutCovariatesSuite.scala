/**
 * Copyright 2016 Taner Dagdelen
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
import net.fnothaft.gnocchi.models.{ Phenotype }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

class LoadPhenotypesWithoutCovariatesSuite extends GnocchiFunSuite {

  sparkTest("Load a simple phenotypes file, including no covariates.") {
    val phenotypes = sc.parallelize(List("Sample1\t1.0\t2.0\t3.0\t4.0\t5.0"))
    val primaryPhenoIndex = 3
    val header = "SampleId\tpheno1\tpheno2\tpheno3\tpheno4\tpheno5"
    val p1 = LoadPhenotypesWithoutCovariates.getAndFilterPhenotypes(phenotypes, header, primaryPhenoIndex, sc)
    assert(p1.first().sampleId === "Sample1")
    assert(p1.first().phenotype === "pheno3")
    assert(p1.first().value === Array(3.0))
  }

  sparkTest("Test file format") {
    val filepath = ClassLoader.getSystemClassLoader.getResource("BadFormatting.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithoutCovariates(filepath, "pheno3", sc)
    }
  }

  sparkTest("Read in a 2-line file") {
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val p1 = LoadPhenotypesWithoutCovariates(filepath, "pheno2", sc)
    assert(p1.first().sampleId === "Sample1", "Sample ID was incorrect")
    assert(p1.first().phenotype === "pheno2", "Phenotype name was incorrect")
    assert(p1.first().value === Array(12.0), "Phenotype value was incorrect")
  }

  sparkTest("Read in a 2-line file; call with phenoName typo") {
    /*
    make sure there is an error thrown if the phenoName doesn't match
    */
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithoutCovariates(filepath, "pheno", sc)
    }
  }

  sparkTest("Read in a 5-line file but with one sample missing the phenotype.") {
    /*
    make sure the right data survive the filters.
    */
    val filepath = ClassLoader.getSystemClassLoader.getResource("MissingPhenotypes.txt").getFile
    val p1 = LoadPhenotypesWithoutCovariates(filepath, "pheno2", sc)
    // assert that it is the right size
    assert(p1.collect().length === 4)
    // assert that the contents are correct 
    assert(p1.collect()(0).sampleId === "Sample1")
    assert(p1.collect()(0).phenotype === "pheno2")
    assert(p1.collect()(0).value === Array(12.0))

    assert(p1.collect()(1).sampleId === "Sample3")
    assert(p1.collect()(1).phenotype === "pheno2")
    assert(p1.collect()(1).value === Array(32.0))

    assert(p1.collect()(2).sampleId === "Sample4")
    assert(p1.collect()(2).phenotype === "pheno2")
    assert(p1.collect()(2).value === Array(42.0))

    assert(p1.collect()(3).sampleId === "Sample5")
    assert(p1.collect()(3).phenotype === "pheno2")
    assert(p1.collect()(3).value === Array(52.0))
  }
}