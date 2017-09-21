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
//
//package net.fnothaft.gnocchi.sql
//
//import net.fnothaft.gnocchi.GnocchiFunSuite
//import net.fnothaft.gnocchi.algorithms.siteregression.{ AdditiveLinearRegression, DominantLinearRegression }
//import net.fnothaft.gnocchi.primitives.genotype.{ Genotype, GenotypeState }
//import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
//import net.fnothaft.gnocchi.primitives.variants.CalledVariant
//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.sql.{ Dataset, SparkSession }
//
//import scala.util.Random
//
//class GnocchiSessionSuite extends GnocchiFunSuite {
//
//  val genoPath = testFile("5snpsFirst5samples.vcf")
//
//  //#CHROM	POS	    ID	      REF	ALT	QUAL	FILTER	INFO	FORMAT	sample1	sample2	sample3	sample4	sample5
//  //1	      752721	rs3131971	A	  G	  60	  PASS	  .	    GT	    0/0	    0/0	    0/0	    0/1	    1/0
//  sparkTest("sc.loadGenotypes should produce a dataset of CalledVariant objects") {
//    val session = new GnocchiSession(sc)
//    val genotypes = session.loadGenotypesAsText("/Users/Nathaniel/bdg/gnocchi/testData/5snps10samples.vcf")
//    assert(genotypes.isInstanceOf[Dataset[CalledVariant]], "LoadGenotypes should produce a" +
//      " Dataset[CalledVariant]")
//  }
//
//  sparkTest("sc.loadGenotypes should map fields correctly") {
//    val session = new GnocchiSession(sc)
//    val genotypes = session.loadGenotypesAsText(genoPath)
//    val firstCalledVariant = genotypes.orderBy("position").head
//
//    assert(firstCalledVariant.uniqueID.equals("rs3131971"))
//    assert(firstCalledVariant.chromosome === 1)
//    assert(firstCalledVariant.position === 752721)
//    assert(firstCalledVariant.referenceAllele === "A")
//    assert(firstCalledVariant.alternateAllele === "G")
//    assert(firstCalledVariant.qualityScore === 60)
//    assert(firstCalledVariant.filter === "PASS")
//    assert(firstCalledVariant.info === ".")
//    assert(firstCalledVariant.format === "GT")
//    assert(firstCalledVariant.samples.equals(List(GenotypeState("sample1", "0/0"),
//      GenotypeState("sample2", "0/0"), GenotypeState("sample3", "0/0"),
//      GenotypeState("sample4", "0/1"), GenotypeState("sample5", "1/0"))))
//  }
//
//  //sc.filterSamples should filter out samples with missing values above a given threshold
//
//  private def makeGenotypeState(id: String, gs: String): GenotypeState = {
//    GenotypeState(id, gs)
//  }
//
//  private def makeCalledVariant(uid: Int, sampleIds: List[String], genotypeStates: List[String]): CalledVariant = {
//    val samples = sampleIds.zip(genotypeStates).map(idGs => makeGenotypeState(idGs._1, idGs._2))
//    CalledVariant(1, 1234, uid.toString(), "A", "G", 60, "PASS", ".", "GT", samples)
//  }
//
//  val sampleIds = List("sample1", "sample2", "sample3", "sample4")
//  val variant1Genotypes = List("./.", "./.", "./.", "1/1")
//  val variant2Genotypes = List("./.", "./.", "1/1", "1/1")
//  val variant3Genotypes = List("./.", "1/1", "1/1", "1/1")
//  val variant4Genotypes = List("./.", "1/1", "1/1", "1/1")
//  val variant5Genotypes = List("./.", "1/1", "1/1", "1/1")
//  val variant1CalledVariant = makeCalledVariant(1, sampleIds, variant1Genotypes)
//  val variant2CalledVariant = makeCalledVariant(2, sampleIds, variant2Genotypes)
//  val variant3CalledVariant = makeCalledVariant(3, sampleIds, variant3Genotypes)
//  val variant4CalledVariant = makeCalledVariant(4, sampleIds, variant4Genotypes)
//  val variant5CalledVariant = makeCalledVariant(5, sampleIds, variant5Genotypes)
//
//  sparkTest("filterSamples should not filter any samples if mind >= 1 since missingness " +
//    "should never exceed 1.0") {
//    val sparkSession = SparkSession.builder().getOrCreate()
//    import sparkSession.implicits._
//
//    val session = new GnocchiSession(sc)
//
//    val calledVariantsDS = sparkSession.createDataFrame(List(variant1CalledVariant, variant2CalledVariant,
//      variant3CalledVariant, variant4CalledVariant, variant5CalledVariant)).as[CalledVariant]
//    val filteredSamples = session.filterSamples(calledVariantsDS, 1.0, 2)
//    assert(filteredSamples.collect.forall(calledVariantsDS.collect.contains(_)), "Sample filtering did not match original collection.")
//  }
//
//  sparkTest("filterSamples should filter on mind if mind is greater than 0 but less than 1") {
//    val sparkSession = SparkSession.builder().getOrCreate()
//    import sparkSession.implicits._
//
//    val session = new GnocchiSession(sc)
//
//    val calledVariantsDS = sparkSession.createDataFrame(List(variant1CalledVariant, variant2CalledVariant,
//      variant3CalledVariant, variant4CalledVariant, variant5CalledVariant)).as[CalledVariant]
//    val filteredSamples = session.filterSamples(calledVariantsDS, .3, 2)
//    val targetFilteredSamples = List("sample3", "sample4")
//    val targetvariant1Genotypes = List("./.", "1/1")
//    val targetvariant2Genotypes = List("1/1", "1/1")
//    val targetvariant3Genotypes = List("1/1", "1/1")
//    val targetvariant4Genotypes = List("1/1", "1/1")
//    val targetvariant5Genotypes = List("1/1", "1/1")
//    val targetvariant1CalledVariant = makeCalledVariant(1, targetFilteredSamples, targetvariant1Genotypes)
//    val targetvariant2CalledVariant = makeCalledVariant(2, targetFilteredSamples, targetvariant2Genotypes)
//    val targetvariant3CalledVariant = makeCalledVariant(3, targetFilteredSamples, targetvariant3Genotypes)
//    val targetvariant4CalledVariant = makeCalledVariant(4, targetFilteredSamples, targetvariant4Genotypes)
//    val targetvariant5CalledVariant = makeCalledVariant(5, targetFilteredSamples, targetvariant5Genotypes)
//    val targetcalledVariantsDS = sparkSession.createDataFrame(List(targetvariant1CalledVariant, targetvariant2CalledVariant,
//      targetvariant3CalledVariant, targetvariant4CalledVariant, targetvariant5CalledVariant)).as[CalledVariant]
//    assert(filteredSamples.collect.forall(targetcalledVariantsDS.collect().contains(_)), "Filtered dataset did not match expected dataset.")
//  }
//
//  sparkTest("filterSamples should filter out all non-perfect samples if mind == 0") {
//    val sparkSession = SparkSession.builder().getOrCreate()
//    import sparkSession.implicits._
//
//    val session = new GnocchiSession(sc)
//
//    val calledVariantsDS = sparkSession.createDataFrame(List(variant1CalledVariant, variant2CalledVariant,
//      variant3CalledVariant, variant4CalledVariant, variant5CalledVariant)).as[CalledVariant]
//    val filteredSamples = session.filterSamples(calledVariantsDS, 0.0, 2)
//    val targetFilteredSamples = List("sample4")
//    val targetvariant1Genotypes = List("1/1")
//    val targetvariant2Genotypes = List("1/1")
//    val targetvariant3Genotypes = List("1/1")
//    val targetvariant4Genotypes = List("1/1")
//    val targetvariant5Genotypes = List("1/1")
//    val targetvariant1CalledVariant = makeCalledVariant(1, targetFilteredSamples, targetvariant1Genotypes)
//    val targetvariant2CalledVariant = makeCalledVariant(2, targetFilteredSamples, targetvariant2Genotypes)
//    val targetvariant3CalledVariant = makeCalledVariant(3, targetFilteredSamples, targetvariant3Genotypes)
//    val targetvariant4CalledVariant = makeCalledVariant(4, targetFilteredSamples, targetvariant4Genotypes)
//    val targetvariant5CalledVariant = makeCalledVariant(5, targetFilteredSamples, targetvariant5Genotypes)
//    val targetcalledVariantsDS = sparkSession.createDataFrame(List(targetvariant1CalledVariant, targetvariant2CalledVariant,
//      targetvariant3CalledVariant, targetvariant4CalledVariant, targetvariant5CalledVariant)).as[CalledVariant]
//    assert(filteredSamples.collect.forall(targetcalledVariantsDS.collect().contains(_)), "Filtered dataset did not match expected dataset.")
//  }
//
//  //  ignore("filterSamples should filter on mind if mind is greater than 0 but less than 1") {
//  //    val allMissingSample = (1 to 10).map(makeGenotypeState(_, "sample1", 0, 2))
//  //    val eachHalfMissingSample = (1 to 10).map(makeGenotypeState(_, "sample2", 1, 1))
//  //    val noneMissingSample = (1 to 10).map(makeGenotypeState(_, "sample3", 2, 0))
//  //    val halfMissingSample = (1 to 5).map(makeGenotypeState(_, "sample4", 2, 0)) ++
//  //      (6 to 10).map(makeGenotypeState(_, "sample4", 0, 2))
//  //
//  //    val badCollection = allMissingSample
//  //    val goodCollection = eachHalfMissingSample ++ halfMissingSample ++ noneMissingSample
//  //
//  //    val gsCollection = goodCollection ++ badCollection
//  //
//  //    val gsRdd = sc.parallelize(gsCollection)
//  //    val resultsRdd = sc.filterSamples(gsRdd, 0.5)
//  //
//  //    assert(goodCollection.toSet == resultsRdd.collect.toSet, "Sample filtering did not match expected collection.")
//  //  }
//  //
//  //  ingore("filterSamples should filter out all non-perfect samples if mind == 0") {
//  //    val allMissingSample = (1 to 10).map(makeGenotypeState(_, "sample1", 0, 2))
//  //    val eachHalfMissingSample = (1 to 10).map(makeGenotypeState(_, "sample2", 1, 1))
//  //    val noneMissingSample = (1 to 10).map(makeGenotypeState(_, "sample3", 2, 0))
//  //    val halfMissingSample = (1 to 5).map(makeGenotypeState(_, "sample4", 2, 0)) ++
//  //      (6 to 10).map(makeGenotypeState(_, "sample4", 0, 2))
//  //
//  //    val imperfectCollection = allMissingSample ++ eachHalfMissingSample ++ halfMissingSample
//  //    val perfectCollection = noneMissingSample
//  //
//  //    val gsCollection = imperfectCollection ++ perfectCollection
//  //
//  //    val gsRdd = sc.parallelize(gsCollection)
//  //    val resultsRdd = sc.filterSamples(gsRdd, 0.0)
//  //
//  //    assert(perfectCollection.toSet == resultsRdd.collect.toSet, "Sample filtering did not match perfect collection.")
//  //  }
//
//  ignore("toGenotypeStateDataFrame should work for ploidy == 1") {
//
//  }
//
//  ignore("toGenotypeStateDataFrame should work for ploidy ==2") {
//
//  }
//
//  ignore("toGenotypeStateDataFrame should count number of NO_CALL's correctly") {
//
//  }
//
//  ignore("filterVariants should not filter any varaints if geno and maf thresholds are both 0") {
//
//  }
//
//  ignore("filterVariants should filter out variants with genotyping rate less than 0.1 when " +
//    "geno threshold set to 0.1 and maf threshold is greater than 0.1") {
//
//  }
//
//  ignore("filterVariants should filter out variants with minor allele frequency less than 0.1 when" +
//    "maf threshold set to 0.1 and geno threshold is greater than 0.1") {
//
//  }
//
//  ignore("Results from loadAndFilterGenotypes should match results of calls of filterSamples and then filterVariants") {
//
//  }
//
//  ignore("loadFileAndCheckHeader should require that the input file is tab or space delimited") {
//
//  }
//
//  ignore("loadFileAndCheckHeader should require that the input file has at least 2 columns") {
//
//  }
//
//  ignore("loadFileAndCheckHeader should require that phenoName and each covariate match a" +
//    "column label in the header") {
//
//  }
//
//  ignore("combineAndFilterPhenotypes should filter out samples that are missing a phenotype or a covariate") {
//
//  }
//
//  ignore("loadPhenotypes should call loadFileAndCheckHeader and thus catch poorly formatted headers") {
//
//  }
//
//  ignore("loadPhenotypes should call combineAndFilterPhenotypes and thus produce combined phenotype objects") {
//
//  }
//
//  ignore("loadPhenotypes should require that phenoName cannot match a provided covar name") {
//
//  }
//
//  ignore("isMissing should yield true iff the value provided is -9.0 or -9") {
//
//  }
//
//  ignore("isMissing should throw a NumberFormatException the value provided cannot be converted to a double") {
//
//  }
//  //
//  //  def dominant(gs: GenotypeState): Double = {
//  //    gs.genotypeState match {
//  //      case 2 => 1.0
//  //      case _ => gs.genotypeState.toDouble
//  //    }
//  //  }
//  //
//  //  def additive(gs: GenotypeState): Double = {
//  //    gs.genotypeState match {
//  //      case _ => gs.genotypeState.toDouble
//  //    }
//  //  }
//
//  ignore("formatObservations should return Array or (genotype, Array[Phenotypes + covariates])") {
//    val gc = new GnocchiContext(sc)
//
//    val genotypeState1 = new Genotype("geno1", 0L, 1L, "A", "G", "sample1", 0, 0, 0)
//    val genotypeState2 = new Genotype("geno2", 1L, 2L, "A", "G", "sample1", 2, 0, 1)
//    val genotypeState3 = new Genotype("geno1", 0L, 1L, "A", "G", "sample2", 0, 0, 0)
//    val genotypeState4 = new Genotype("geno2", 1L, 2L, "A", "G", "sample2", 2, 0, 1)
//    //TODO: Note - if the phase set id's for the same genotype are not consistent, it causes there to only be one genotype present after the sortByKey
//    val phenotype1 = new Phenotype("pheno", "sample1", Array(0.0, 1.0, 2.0))
//    val phenotype2 = new Phenotype("pheno", "sample2", Array(3.0, 4.0, 5.0))
//
//    val genoRDD = sc.parallelize(Array(genotypeState1, genotypeState2, genotypeState3, genotypeState4))
//    val phenoRDD = sc.parallelize(Array(phenotype1, phenotype2))
//
//    //    RDD[((Variant, String, Int), Array[(Double, Array[Double])]
//
//    val additiveFormattedObservations = gc.formatObservations(genoRDD, phenoRDD, AdditiveLinearRegression.clipOrKeepState).collect
//    val dominantFormattedObservations = gc.formatObservations(genoRDD, phenoRDD, DominantLinearRegression.clipOrKeepState).collect
//
//    assert(additiveFormattedObservations(0)._1._1.getContigName == "geno2", "Variant incorrect")
//    assert(additiveFormattedObservations(1)._1._1.getContigName == "geno1", "Variant incorrect")
//    assert(additiveFormattedObservations(0)._2(0)._1 == 2.0, "Genotype incorrect")
//    assert(additiveFormattedObservations(0)._2(1)._1 == 2.0, "Genotype incorrect")
//    assert(additiveFormattedObservations(0)._2(1)._2 sameElements Array(0.0, 1.0, 2.0), "Phenotype array incorrect")
//    assert(additiveFormattedObservations(0)._2(0)._2 sameElements Array(3.0, 4.0, 5.0), "Phenotype array incorrect")
//    assert(additiveFormattedObservations(1)._2(0)._1 == 0.0, "Genotype incorrect")
//    assert(additiveFormattedObservations(1)._2(1)._1 == 0.0, "Genotype incorrect")
//    assert(additiveFormattedObservations(1)._2(1)._2 sameElements Array(0.0, 1.0, 2.0), "Phenotype array incorrect")
//    assert(additiveFormattedObservations(1)._2(0)._2 sameElements Array(3.0, 4.0, 5.0), "Phenotype array incorrect")
//
//    assert(dominantFormattedObservations(0)._1._1.getContigName == "geno2", "Variant incorrect")
//    assert(dominantFormattedObservations(1)._1._1.getContigName == "geno1", "Variant incorrect")
//    assert(dominantFormattedObservations(0)._2(0)._1 == 1.0, "Genotype incorrect")
//    assert(dominantFormattedObservations(0)._2(1)._1 == 1.0, "Genotype incorrect")
//    assert(dominantFormattedObservations(0)._2(1)._2 sameElements Array(0.0, 1.0, 2.0), "Phenotype array incorrect")
//    assert(dominantFormattedObservations(0)._2(0)._2 sameElements Array(3.0, 4.0, 5.0), "Phenotype array incorrect")
//    assert(dominantFormattedObservations(1)._2(0)._1 == 0.0, "Genotype incorrect")
//    assert(dominantFormattedObservations(1)._2(1)._1 == 0.0, "Genotype incorrect")
//    assert(dominantFormattedObservations(1)._2(1)._2 sameElements Array(0.0, 1.0, 2.0), "Phenotype array incorrect")
//    assert(dominantFormattedObservations(1)._2(0)._2 sameElements Array(3.0, 4.0, 5.0), "Phenotype array incorrect")
//  }
//}