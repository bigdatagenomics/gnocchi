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
import net.fnothaft.gnocchi.association._
import java.io.File
import java.nio.file.Files

class RegressPhenotypesSuite extends GnocchiFunSuite {

  val path = "src/test/resources/testData/Association"
  val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/" + path

  sparkTest("Test LoadPhenotypes: Read in a 2-line phenotype file; call with one of the covariate names same as pheno name") {
    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    intercept[AssertionError] {
      val p1 = LoadPhenotypesWithCovariates(false, filepath, filepath, "pheno2", "pheno2,pheno4", sc)
    }
  }

  sparkTest("Test LoadPhenotypes: Read in a 2-line phenotype file; call with ADDDITIVE_LINEAR but with no phenoName") {
    val genoFilePath = "File://" + ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val phenoFilePath = "File://" + ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val baseDir = new File(".").getAbsolutePath()
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -overwriteParquet -oneTwo"
    val cliArgs = cliCall.split(" ").drop(2)
    intercept[AssertionError] {
      RegressPhenotypes(cliArgs).run(sc)
    }
  }

  sparkTest("Test LoadPhenotypes: Read in a 2-line phenotype file; call with -covar but not -covarNames") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    intercept[AssertionError] {
      RegressPhenotypes(cliArgs).run(sc)
    }
  }

  sparkTest("Test loadGenotypes: output from vcf input") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno2 -covar -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    val genotypeStateDataset = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val genotypeStateArray = genotypeStateDataset.collect()
    val genotypeState = genotypeStateArray(0)
    assert(genotypeState.start === 14521, "GenotypeState start is incorrect: " + genotypeState.start)
    assert(genotypeState.end === 14522, "GenotypeState end is incorrect: " + genotypeState.end)
    assert(genotypeState.ref === "G", "GenotypeState ref is incorrect: " + genotypeState.ref)
    assert(genotypeState.alt === "A", "GenotypeState alt is incorrect: " + genotypeState.alt)
    assert(genotypeState.sampleId === "sample1", "GenotypeState sampleId is incorrect: " + genotypeState.sampleId)
    assert(genotypeState.genotypeState === 1, "GenotypeState genotypeState is incorrect: " + genotypeState.genotypeState)
  }

  sparkTest("Test full pipeline: 1 snp, 10 samples, 1 phenotype, no covars") {
    // import AssociationEncoder._
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("1snp10samples.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples1Phenotype.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    val genotypeStates = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val phenotypes = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
    val regressionResult = RegressPhenotypes(cliArgs).performAnalysis(genotypeStates, phenotypes, sc).collect()

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult(0).statistics("rSquared") == 1.0, "rSquared = " + regressionResult(0).statistics("rSquared"))
  }
  sparkTest("Test full pipeline: 1 snp, 10 samples, 12 samples in phenotype file (10 matches) 1 phenotype, no covar") {
    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("1snp10samples.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("12samples1Phenotype.txt").getFile
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    val genotypeStates = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val phenotypes = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
    val regressionResult = RegressPhenotypes(cliArgs).performAnalysis(genotypeStates, phenotypes, sc).collect()

    //Assert that the rsquared is in the right threshold. 
    assert(regressionResult(0).statistics("rSquared") == 1.0, "rSquared = " + regressionResult(0).statistics("rSquared"))
  }

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

    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("5snps10samples.vcf").getFile
    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
    val covarFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
    println(genoFilePath)
    // val destination = "~/Users/Taner/desktop/associations"
    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
    val cliArgs = cliCall.split(" ").drop(2)
    val genotypeStates = RegressPhenotypes(cliArgs).loadGenotypes(sc)
    val phenotypes = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
    val assocs = RegressPhenotypes(cliArgs).performAnalysis(genotypeStates, phenotypes, sc)
    val regressionResult = assocs.collect()

    for (result <- regressionResult) {
      println("SNP Name: " + result.variant.getContig.getContigName)
      println("SNP Locus: " + result.variant.getStart + "-" + result.variant.getEnd)
      println("Phenotypes: " + result.phenotype)
      println("logPValue: " + result.logPValue)
    }

    RegressPhenotypes(cliArgs).logResults(assocs, sc)
    //Assert that the rsquared is in the right threshold. 
    //    assert(regressionResult(0).statistics("rSquared") == 0.7681191628941112, "rSquared = " + regressionResult(0).statistics("rSquared"))
    assert(regressionResult(0).statistics("rSquared") == 0.833277921795612, "rSquared = " + regressionResult(0).statistics("rSquared"))

  }

}
// genoFilePath 
// ./bin/gnocchi-submit regressPhenotypes gnocchi-cli/target/test-classes/5snps10samples.vcf gnocchi-cli/target/test-classes/10samples5Phenotypes2covars.txt ADDITIVE_LINEAR TestDataResults -saveAsText -phenoName pheno1 -covar -covarNames pheno4,pheno5 -overwriteParquet
