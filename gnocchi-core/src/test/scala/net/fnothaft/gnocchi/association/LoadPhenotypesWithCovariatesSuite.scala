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

class LoadPhenotypesWithCovariatesSuite extends GnocchiFunSuite {

  /*
  Need to test LoadPhenotypesWithCovariates and RegressPhenotypes as a whole
    - Load a simple phenotypes example manually
    - Test file format
      - Read in a 2-line file and make sure an error is thrown if it is comma delimited and not tab delimited.
    - Read in a 2-line file
      - make sure the right labels and values are set based on the CLI arguments that were given
    - Read in a 2-line file; call with phenoName typo
      - make sure there is an error thrown if the phenoName doesn't match
    - Read in a 2-line file; call with covarNames typo
      - make sure there is an error thrown if one of the covarNames doesn't match
    - Read in a 5-line file but one sample should be missing the phenotype and two others two different covariates.
      - make sure the right data survive the filters.
  */

  sparkTest("Load a simple phenotypes example manually") {
    val phenotypes = sc.parallelize(List("Sample1\t1.0\t2.0\t3.0\t4.0\t5.0"))
    val primaryPhenoIndex = 3
    val covarIndices = Array(1, 2, 5)
    val header = "SampleId\tpheno1\tpheno2\tpheno3\tpheno4\tpheno5"
    val p1 = LoadPhenotypesWithCovariates.getAndFilterPhenotypes(phenotypes, header, primaryPhenoIndex, covarIndices, sc)
    assert(p1.first().sampleId === "Sample1")
    assert(p1.first().phenotype === "pheno3,pheno1,pheno2,pheno5")
    assert(p1.first().value === Array(3.0, 1.0, 2.0, 5.0))
  }

  sparkTest("Test file format") {
    val filepath = ClassLoader.getSystemClassLoader.getResource("BadFormatting.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithCovariates(filepath, "pheno3", "pheno1,pheno2,pheno5", sc)
    }
  }

  sparkTest("Read in a 2-line file") {
    /*
    - Read in a 2-line file
      - make sure the right labels and values are set based on the CLI arguments that were given
    */
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val p1 = LoadPhenotypesWithCovariates(filepath, "pheno2", "pheno3,pheno4", sc)
    assert(p1.first().sampleId === "Sample1", "Sample ID was incorrect")
    assert(p1.first().phenotype === "pheno2,pheno3,pheno4", "Phenotype name was incorrect")
    assert(p1.first().value === Array(12.0, 13.0, 14.0), "Phenotype value was incorrect")
  }

  sparkTest("Read in a 2-line file; call with phenoName typo") {
    /*
    make sure there is an error thrown if the phenoName doesn't match
    */
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithCovariates(filepath, "pheno", "pheno3,pheno4", sc)
    }
    // assert(throwsError, "AssertionError should be thrown if user inputs a pheno name that doesn't match anything in the header.")
  }

  sparkTest("Read in a 2-line file; call with covarNames typo") {
    /*
    make sure there is an error thrown if one of the covarNames doesn't match
    */
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithCovariates(filepath, "pheno2", "pheno,pheno4", sc)
    }
    // assert(throwsError, "AssertionError should be thrown if user inputs a covarName that doesn't match anything in the header.")
  }

  sparkTest("Read in a 5-line file but with one sample missing the phenotype and two others two different covariates.") {
    /*
    make sure the right data survive the filters.
    */
    val filepath = ClassLoader.getSystemClassLoader.getResource("MissingPhenotypes.txt").getFile
    val p1 = LoadPhenotypesWithCovariates(filepath, "pheno2", "pheno3,pheno4", sc)
    // assert that it is the right size
    assert(p1.collect().length === 2)
    // assert that the contents are correct 
    assert(p1.collect()(0).sampleId === "Sample1")
    assert(p1.collect()(0).phenotype === "pheno2,pheno3,pheno4")
    assert(p1.collect()(0).value === Array(12.0, 13.0, 14.0))

    assert(p1.collect()(1).sampleId === "Sample5")
    assert(p1.collect()(1).phenotype === "pheno2,pheno3,pheno4")
    assert(p1.collect()(1).value === Array(52.0, 53.0, 54.0))
  }
}
