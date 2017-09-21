///**
// * Licensed to Big Data Genomics (BDG) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The BDG licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package net.fnothaft.gnocchi.cli
//
//import net.fnothaft.gnocchi.GnocchiFunSuite
//import net.fnothaft.gnocchi.algorithms._
//import java.io.File
//import java.nio.file.Files
//
//import net.fnothaft.gnocchi.algorithms.siteregression.AdditiveLinearRegression
//import net.fnothaft.gnocchi.sql.GnocchiContext._
//import org.kohsuke.args4j.CmdLineException
//
//class RegressPhenotypesSuite extends GnocchiFunSuite {
//
//  val path = "src/test/resources/testData/Association"
//  val destination = Files.createTempDirectory("").toAbsolutePath.toString + "/Association"
//
//  ignore("Test LoadPhenotypes: Read in a 2-line phenotype file; call with one of the covariate names same as pheno name") {
//    val filepath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
//  }
//
//  ignore("Test LoadPhenotypes: Read in a 2-line phenotype file; call with ADDDITIVE_LINEAR but with no phenoName") {
//    val genoFilePath = "File://" + ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
//    val phenoFilePath = "File://" + ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
//    val baseDir = new File(".").getAbsolutePath()
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -overwriteParquet -oneTwo"
//    val cliArgs = cliCall.split(" ").drop(2)
//
//    RegressPhenotypes(cliArgs).run(sc)
//
//  }
//
//  ignore("Test LoadPhenotypes: Read in a 2-line phenotype file; call with -covar but not -covarNames") {
//    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
//    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("2Liner.txt").getFile
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -phenoName pheno2 -covar -overwriteParquet"
//    val cliArgs = cliCall.split(" ").drop(2)
//    intercept[IllegalArgumentException] {
//      RegressPhenotypes(cliArgs).run(sc)
//    }
//  }
//
//  ignore("Test full pipeline: 1 snp, 10 samples, 1 phenotype, no covars") {
//    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("1snp10samples.vcf").getFile
//    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples1Phenotype.txt").getFile
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -overwriteParquet"
//    val cliArgs = cliCall.split(" ").drop(2)
//    val genotypeStates = sc.loadAndFilterGenotypes(genoFilePath, destination, 1, 0.1, 0.1, 0.1, false)
//    val phenotypes = sc.loadPhenotypes(phenoFilePath, "pheno1", false, false, Option.empty[String], Option.empty[String])
//    val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, AdditiveLinearRegression.clipOrKeepState)
//    val regressionResult = AdditiveLinearRegression(genoPhenoObs).collect()
//
//    //Assert that the rsquared is in the right threshold.
//    assert(regressionResult(0).statistics("rSquared") == 1.0, "rSquared = " + regressionResult(0).statistics("rSquared"))
//  }
//
//  ignore("Test full pipeline: 1 snp, 10 samples, 12 samples in phenotype file (10 matches) 1 phenotype, no covar") {
//    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("1snp10samples.vcf").getFile
//    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("12samples1Phenotype.txt").getFile
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -overwriteParquet"
//    val cliArgs = cliCall.split(" ").drop(2)
//    val genotypeStates = sc.loadAndFilterGenotypes(genoFilePath, destination, 1, 0.1, 0.1, 0.1, false)
//    val phenotypes = sc.loadPhenotypes(phenoFilePath, "pheno1", false, false, Option.empty[String], Option.empty[String])
//    val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, AdditiveLinearRegression.clipOrKeepState)
//    val regressionResult = AdditiveLinearRegression(genoPhenoObs).collect()
//
//    //Assert that the rsquared is in the right threshold.
//    assert(regressionResult(0).statistics("rSquared") == 1.0, "rSquared = " + regressionResult(0).statistics("rSquared"))
//  }
//
//  ignore("Test full pipeline: 5 snps, 10 samples, 1 phenotype, 2 random noise covars") {
//    /*
//     Uniform White Noise for Covar 1 (pheno4):
//      0.8404
//     -0.8880
//      0.1001
//     -0.5445
//      0.3035
//     -0.6003
//      0.4900
//      0.7394
//      1.7119
//     -0.1941
//    Uniform White Noise for Covar 2 (pheno5):
//      2.9080
//      0.8252
//      1.3790
//     -1.0582
//     -0.4686
//     -0.2725
//      1.0984
//     -0.2779
//      0.7015
//     -2.0518
//   */
//
//    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("5snps10samples.vcf").getFile
//    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
//    val covarFilePath = ClassLoader.getSystemClassLoader.getResource("10samples5Phenotypes2covars.txt").getFile
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -covar -covarFile $covarFilePath -covarNames pheno4,pheno5 -overwriteParquet"
//    val cliArgs = cliCall.split(" ").drop(2)
//    val genotypeStates = sc.loadAndFilterGenotypes(genoFilePath, destination, 1, 0.1, 0.1, 0.1, false)
//    val phenotypes = sc.loadPhenotypes(phenoFilePath, "pheno1", false, true, Option(covarFilePath), Option("pheno4,pheno5"))
//    val genoPhenoObs = sc.formatObservations(genotypeStates, phenotypes, AdditiveLinearRegression.clipOrKeepState)
//    val assocs = AdditiveLinearRegression(genoPhenoObs)
//    val regressionResult = assocs.collect()
//
//    assert(regressionResult(0).statistics("rSquared") == 0.8438315575507651, "rSquared = " + regressionResult(0).statistics("rSquared"))
//
//  }
//
//  ignore("Test that singular matrix exceptions are caught: LENIENT Case") {
//    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("SingularGeno.vcf").getFile
//    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("SingularPheno.txt").getFile
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -overwriteParquet -validationStringency LENIENT"
//    val cliArgs = cliCall.split(" ").drop(2)
//    //    val genotypeStates = RegressPhenotypes(cliArgs).loadGenotypes(sc)
//    //    val phenotypes = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
//    //    val regressionResult = RegressPhenotypes(cliArgs).performAnalysis(genotypeStates, phenotypes, sc).collect()
//  }
//
//  ignore("Test that singular matrix exceptions are caught: STRICT Case") {
//    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("SingularGeno.vcf").getFile
//    val phenoFilePath = ClassLoader.getSystemClassLoader.getResource("SingularPheno.txt").getFile
//    val cliCall = s"../bin/gnocchi-submit regressPhenotypes $genoFilePath $phenoFilePath ADDITIVE_LINEAR $destination -saveAsText -phenoName pheno1 -overwriteParquet -validationStringency STRICT -maf 0 -geno 0 -mind 0"
//    val cliArgs = cliCall.split(" ").drop(2)
//    //    val genotypeStates = RegressPhenotypes(cliArgs).loadGenotypes(sc)
//    //    val phenotypes = RegressPhenotypes(cliArgs).loadPhenotypes(sc)
//    //    val regressionResult = RegressPhenotypes(cliArgs).performAnalysis(genotypeStates, phenotypes, sc).collect()
//  }
//}
