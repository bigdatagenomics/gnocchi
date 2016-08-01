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
package net.fnothaft.gnocchi.cli

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.{ Phenotype }
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
// import sys.process._
import net.fnothaft.gnocchi.association._
import net.fnothaft.gnocchi.models.GenotypeState

class RegressPhenotypesSuite extends GnocchiFunSuite {

  sparkTest("Test LoadPhenotypes: Read in a 2-line phenotype file; call with one of the covariate names same as pheno name") {
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithCovariates(filepath, "pheno2", "pheno2,pheno4", sc)
    } //    assert(throwsError,"AssertionError should be thrown if user tries to include the same variable as both phenoName and a covariate")
  }

  sparkTest("Test LoadPhenotypes: Read in a 2-line phenotype file; call with ADDDITIVE_LINEAR but with no phenoName") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val destination = "src/test/resources/testData/Association"
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    intercept[AssertionError] {
      RegressPhenotypes(cliArgs).run(sc)
    }
  }

  sparkTest("Test LoadPhenotypes: Read in a 2-line phenotype file; call with -covar but not -covarNames") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val destination = "src/test/resources/testData/Association"
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    intercept[AssertionError] {
      RegressPhenotypes(cliArgs).run(sc)
    }
  }

  sparkTest("Test loadGenotypes: output from vcf input") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val destination = "src/test/resources/testData/Association"
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    // println("first() is the problem")
    val genotypeStateArray = genotypeStateDataset.collect()
    println(genotypeStateArray)
    println(genotypeStateArray.length)
    val genotypeState = genotypeStateArray(0)
    // println(genoState)
    // println(genoState.mkString(", "))
    // val genotypeState = genoState(0)
    // println("one of the assertions is the problem")
    assert(genotypeState.start === 14522, "GenotypeState start is incorrect: " + genotypeState.start)
    assert(genotypeState.end === 14523, "GenotypeState end is incorrect: " + genotypeState.end)
    assert(genotypeState.ref === "G", "GenotypeState ref is incorrect: " + genotypeState.ref)
    assert(genotypeState.alt === "A", "GenotypeState alt is incorrect: " + genotypeState.alt)
    assert(genotypeState.sampleId === ".", "GenotypeState sampleId is incorrect: " + genotypeState.sampleId)
    assert(genotypeState.genotypeState === 1, "GenotypeState genotypeState is incorrect: " + genotypeState.genotypeState)
  }

  sparkTest("Test full pipeline: 1 snp, 10 samples, 1 phenotype, no covars") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("1snp10samples.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples1Phenotype.txt").getFile
    val destination = "src/test/resources/testData/Association"
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    val genotypeStates = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val phenotypes = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
    val regressionResult = RegressPhenotypes(cliArgs).performAnalysis(genotypeStates, phenotypes, sc).collect()
    println(regressionResult)
    println(regressionResult.length)

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult(0).statistics("rSquared") == 1.0, "rSquared = " + regressionResult(0).statistics("rSquared"))
  }
  sparkTest("Test full pipeline: 1 snp, 10 samples, 12 samples in phenotype file (10 matches) 1 phenotype, no covar") {}
  sparkTest("Test full pipeline: 1 snp, 10 samples, 1 phenotype, 1 covar") {}
  sparkTest("Test full pipeline: 5 snps, 10 samples, 1 phenotype, 2 random noise covars") {
    /* 
     Uniform White Noise for Covar 1 (pheno4): 
      0.8404
     -0.8880
      0.1001
     -0.5445
      0.3035
     -0.6003
      0.4900
      0.7394
      1.7119
     -0.1941
    Uniform White Noise for Covar 2 (pheno5): 
      2.9080
      0.8252
      1.3790
     -1.0582
     -0.4686
     -0.2725
      1.0984
     -0.2779
      0.7015
     -2.0518
   */
  }
  //TO DO: tests for filtering out the proper SNPs and Samples using MAF, Missingness, and Genorate thresholds

  // val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
  // val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
  // val destination = "./testData/Association"
  // val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -phenoName pheno2 -overWriteParquet"
  // val cliArgs = cliCall.split(" ").drop(2)
  // val associations = RegressPhenotypes(cliArgs).run(sc)
  // println(associations.count())

  sparkTest("Test runAnalysis from hard-coded genotypeStateDataset") {

    // Create a genotypeStates
    //        GenotypeState(contig: String,
    //                      start: Long,
    //                      end: Long,
    //                      ref: String,
    //                      alt: String,
    //                      sampleId: String,
    //                      genotypeState: Int)
    println("1")

    val genotypeState1 = new GenotypeState(contig = null, start = 1, end = 2, ref = "G", alt = "A", sampleId = "sample1", genotypeState = 1)
    val genotypeState2 = new GenotypeState(contig = null, start = 1, end = 2, ref = "G", alt = "A", sampleId = "sampel2", genotypeState = 0)

    // create phenotypes RDD
    println("2")
    val phenotypes = sc.parallelize(List("Sample1\t1.0", "Sample2\t1.0"))
    val primaryPhenoIndex = 1
    println("2.5")
    val covarIndices = Array(1, 2, 5)
    val header = "SampleId\tpheno1"
    println("3")
    val p1 = LoadPhenotypesWithCovariates.getAndFilterPhenotypes(phenotypes, header, primaryPhenoIndex, covarIndices, sc)
    // create dataset form genotypeStates
    println("4")
  }

}
