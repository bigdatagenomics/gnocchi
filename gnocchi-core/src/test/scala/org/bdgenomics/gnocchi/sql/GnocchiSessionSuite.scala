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

import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.util.Random

class GnocchiSessionSuite extends GnocchiFunSuite {

  // load Genotypes tests

  sparkTest("sc.loadGenotypes should produce a dataset of CalledVariant objects.") {
    val genoPath = testFile("small1.vcf")
    val genotypes = sc.loadGenotypes(genoPath)
    assert(genotypes.isInstanceOf[Dataset[CalledVariant]], "sc.loadGenotypes does not produce as Dataset[CalledVariant]")
  }

  sparkTest("sc.loadGenotypes should map fields correctly.") {
    val genoPath = testFile("1Sample1Variant.vcf")
    val genotypes = sc.loadGenotypes(genoPath)
    val firstCalledVariant = genotypes.head

    assert(firstCalledVariant.uniqueID.equals("rs3131972"))
    assert(firstCalledVariant.chromosome === 1)
    assert(firstCalledVariant.position === 752721)
    assert(firstCalledVariant.referenceAllele === "A")
    assert(firstCalledVariant.alternateAllele === "G")
    assert(firstCalledVariant.samples.equals(List(GenotypeState("sample1", "0/1"))))
  }

  sparkTest("sc.loadGenotypes should gracefully exit when a non-existing file path is passed in.") {
    val fakeFilePath = "fake/file/path.vcf"
    try {
      sc.loadGenotypes(fakeFilePath)
      fail("sc.loadGenotypes does not fail on a fake file path.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  ignore("sc.loadGenotypes should have no overlapping values in the `uniqueID` field.") {

  }

  ignore("sc.loadGenotypes should be able to take in ADAM formatted parquet files with genotype states.") {

  }

  // load phenotypes tests

  sparkTest("sc.loadPhenotypes should gracefully exit when a non-existing file path is passed in.") {
    val fakeFilePath = "fake/file/path.vcf"
    try {
      sc.loadPhenotypes(fakeFilePath, "IID", "pheno", "\t")
      fail("sc.loadPhenotypes does not fail on a fake file path.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when the specified primaryID is not a column in the phenotypes file.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "IID", "pheno", "\t")
      fail("sc.loadPhenotypes does not fail on a non-present primary sample ID.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when passed an invalid phenotype delimiter.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "pheno1", ",")
      fail("sc.loadPhenotypes does not fail on invalid delimiter.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when passed an invalid covariate delimiter.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", Option(path), Option(List("pheno2", "pheno3")), covarDelimiter = ",")
      fail("sc.loadPhenotypes does not fail on invalid covariate delimiter.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when the primary phenotype is listed as a covariate.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", Option(path), Option(List("pheno1", "pheno3")), covarDelimiter = "\t")
      fail("sc.loadPhenotypes does not fail on primary phenotype listed as a covariate.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when a covariates path is passed in without covariate names.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", Option(path), covarDelimiter = "\t")
      fail("sc.loadPhenotypes does not fail on a covariates path is passed in without covariate names.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when covariate names are passed in without a covariates path.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", covarNames = Option(List("pheno2", "pheno3")), covarDelimiter = "\t")
      fail("sc.loadPhenotypes does not fail on covariate names passed in without covariates path.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when a covariate name is not a column in the covariate file.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", Option(path), Option(List("notPresentPhenotype", "pheno3")), covarDelimiter = "\t")
      fail("sc.loadPhenotypes does not fail when a covariate name is not a column in the covariate file.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should gracefully exit when primary phenotype is not a column in the phenotype file.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    try {
      sc.loadPhenotypes(path, "SampleID", "notPresentPhenotype", "\t", Option(path), Option(List("pheno2", "pheno3")), covarDelimiter = "\t")
      fail("sc.loadPhenotypes does not fail when a covariate name is not a column in the covariate file.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.loadPhenotypes should produce a scala `Map[String, Phenotype]`.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    val pheno = sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", Option(path), Option(List("pheno2", "pheno3")), covarDelimiter = "\t")
    assert(pheno.isInstanceOf[Map[String, Phenotype]], "sc.loadPhenotypes does not produce a `Map[String, Phenotype]`")
  }

  sparkTest("sc.loadPhenotypes should properly load in covariates.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    val pheno = sc.loadPhenotypes(path, "SampleID", "pheno1", "\t", Option(path), Option(List("pheno4", "pheno5")), covarDelimiter = "\t")
    assert(pheno("sample1").covariates == List(0.8404, 2.9080), "sc.loadPhenotypes does not load in proper covariates: sample1")
    assert(pheno("sample2").covariates == List(-0.8880, 0.8252), "sc.loadPhenotypes does not load in proper covariates: sample2")
    assert(pheno("sample3").covariates == List(0.1001, 1.3790), "sc.loadPhenotypes does not load in proper covariates: sample3")
    assert(pheno("sample4").covariates == List(-0.5445, -1.0582), "sc.loadPhenotypes does not load in proper covariates: sample4")
    assert(pheno("sample5").covariates == List(0.3035, -0.4686), "sc.loadPhenotypes does not load in proper covariates: sample5")
  }

  sparkTest("sc.loadPhenotypes should create empty lists in the covariate field of the Phenotype objects if there are no covariates.") {
    val path = testFile("first5samples5phenotypes2covars.txt")
    val pheno = sc.loadPhenotypes(path, "SampleID", "pheno1", "\t")
    assert(pheno("sample1").covariates == List(), "sc.loadPhenotypes does not load in proper covariates: sample1")
    assert(pheno("sample2").covariates == List(), "sc.loadPhenotypes does not load in proper covariates: sample2")
    assert(pheno("sample3").covariates == List(), "sc.loadPhenotypes does not load in proper covariates: sample3")
    assert(pheno("sample4").covariates == List(), "sc.loadPhenotypes does not load in proper covariates: sample4")
    assert(pheno("sample5").covariates == List(), "sc.loadPhenotypes does not load in proper covariates: sample5")
  }

  // filter samples tests

  private def makeGenotypeState(id: String, gs: String): GenotypeState = {
    GenotypeState(id, gs)
  }

  private def makeCalledVariant(uid: Int, sampleIds: List[String], genotypeStates: List[String]): CalledVariant = {
    val samples = sampleIds.zip(genotypeStates).map(idGs => makeGenotypeState(idGs._1, idGs._2))
    CalledVariant(1, 1234, uid.toString(), "A", "G", "60", "PASS", ".", "GT", samples)
  }

  private def makeCalledVariantDS(variants: List[CalledVariant]): Dataset[CalledVariant] = {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    sc.parallelize(variants).toDS()
  }

  val sampleIds = List("sample1", "sample2", "sample3", "sample4")
  val variant1Genotypes = List("./.", "./.", "./.", "1/1")
  val variant2Genotypes = List("./.", "./.", "1/1", "1/1")
  val variant3Genotypes = List("./.", "1/1", "1/1", "1/1")
  val variant4Genotypes = List("./.", "1/1", "1/1", "1/1")
  val variant5Genotypes = List("./.", "1/1", "1/1", "1/1")
  val variant1CalledVariant = makeCalledVariant(1, sampleIds, variant1Genotypes)
  val variant2CalledVariant = makeCalledVariant(2, sampleIds, variant2Genotypes)
  val variant3CalledVariant = makeCalledVariant(3, sampleIds, variant3Genotypes)
  val variant4CalledVariant = makeCalledVariant(4, sampleIds, variant4Genotypes)
  val variant5CalledVariant = makeCalledVariant(5, sampleIds, variant5Genotypes)
  val filterVariants = List(variant1CalledVariant, variant2CalledVariant,
    variant3CalledVariant, variant4CalledVariant, variant5CalledVariant)

  sparkTest("sc.filterSamples should not filter any samples if mind >= 1 since missingness should never exceed 1.0") {
    val toFilterDS = makeCalledVariantDS(filterVariants)
    val filteredSamples = sc.filterSamples(toFilterDS, 1.0, 2)
    assert(filteredSamples.collect.forall(toFilterDS.collect.contains(_)), "Sample filtering did not match original collection.")
  }

  sparkTest("sc.filterSamples should filter on mind if mind is greater than 0 but less than 1") {
    val toFilterDS = makeCalledVariantDS(filterVariants)
    val filteredSamples = sc.filterSamples(toFilterDS, .3, 2)

    val targetFilteredSamples = List("sample3", "sample4")
    val targetvariant1Genotypes = List("./.", "1/1")
    val targetvariant2Genotypes = List("1/1", "1/1")
    val targetvariant3Genotypes = List("1/1", "1/1")
    val targetvariant4Genotypes = List("1/1", "1/1")
    val targetvariant5Genotypes = List("1/1", "1/1")
    val targetvariant1CalledVariant = makeCalledVariant(1, targetFilteredSamples, targetvariant1Genotypes)
    val targetvariant2CalledVariant = makeCalledVariant(2, targetFilteredSamples, targetvariant2Genotypes)
    val targetvariant3CalledVariant = makeCalledVariant(3, targetFilteredSamples, targetvariant3Genotypes)
    val targetvariant4CalledVariant = makeCalledVariant(4, targetFilteredSamples, targetvariant4Genotypes)
    val targetvariant5CalledVariant = makeCalledVariant(5, targetFilteredSamples, targetvariant5Genotypes)

    val targetcalledVariantsDS = makeCalledVariantDS(List(targetvariant1CalledVariant, targetvariant2CalledVariant,
      targetvariant3CalledVariant, targetvariant4CalledVariant, targetvariant5CalledVariant))
    assert(filteredSamples.collect.forall(targetcalledVariantsDS.collect().contains(_)), "Filtered dataset did not match expected dataset.")
  }

  sparkTest("sc.filterSamples should filter out all non-perfect samples if mind == 0") {
    val toFilterDS = makeCalledVariantDS(filterVariants)
    val filteredSamples = sc.filterSamples(toFilterDS, 0.0, 2)
    val targetFilteredSamples = List("sample4")
    val targetvariant1Genotypes = List("1/1")
    val targetvariant2Genotypes = List("1/1")
    val targetvariant3Genotypes = List("1/1")
    val targetvariant4Genotypes = List("1/1")
    val targetvariant5Genotypes = List("1/1")
    val targetvariant1CalledVariant = makeCalledVariant(1, targetFilteredSamples, targetvariant1Genotypes)
    val targetvariant2CalledVariant = makeCalledVariant(2, targetFilteredSamples, targetvariant2Genotypes)
    val targetvariant3CalledVariant = makeCalledVariant(3, targetFilteredSamples, targetvariant3Genotypes)
    val targetvariant4CalledVariant = makeCalledVariant(4, targetFilteredSamples, targetvariant4Genotypes)
    val targetvariant5CalledVariant = makeCalledVariant(5, targetFilteredSamples, targetvariant5Genotypes)

    val targetcalledVariantsDS = makeCalledVariantDS(List(targetvariant1CalledVariant, targetvariant2CalledVariant,
      targetvariant3CalledVariant, targetvariant4CalledVariant, targetvariant5CalledVariant))

    assert(filteredSamples.collect.forall(targetcalledVariantsDS.collect().contains(_)), "Filtered dataset did not match expected dataset.")
  }

  sparkTest("sc.filterSamples exit gracefully when mind < 0.") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List.fill(5)(createSampleCalledVariant()))
    try {
      sc.filterSamples(dataset, -0.4, 2)
      fail("sc.filterSamples does not fail on missingness per individual < 0.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  ignore("sc.filterSamples should remove samples from return dataset if they do not have a phenotype.") {

  }

  // filter variants tests

  sparkTest("sc.filterVariants exit gracefully when maf _ > 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List.fill(5)(createSampleCalledVariant()))
    try {
      sc.filterVariants(dataset, geno = 0.0, maf = 1.2)
      fail("sc.filterVariants does not fail on minor allele frequency > 1.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.filterVariants should filter out all samples when maf == 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List.fill(5)(createSampleCalledVariant()))
    val leftOver = sc.filterVariants(dataset, geno = 0.0, maf = 1.0)
    assert(leftOver.count() == 0, "Minor allele frequency filter at 1.0 should filter out all variants.")
  }

  sparkTest("sc.filterVariants should properly filter out variants when  0 < maf < 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val mafs = List(0.2, 0.4, 0.6, 0.8)
      .map(x => createSampleGenotypeStates(num = 10, maf = x))
      .map(x => createSampleCalledVariant(samples = Option(x)))
    val dataset = sparkSession.createDataset(mafs)

    def maf1 = sc.filterVariants(dataset, maf = 0.2, geno = 0.0)
    def maf2 = sc.filterVariants(dataset, maf = 0.4, geno = 0.0)

    assert(maf1.count() == 3, "Minor allele frequency filtered out wrong number.")
    assert(maf2.count() == 2, "Minor allele frequency filtered out wrong number.")
  }

  ignore("sc.filterVariants should filter out everything when maf > 0.5") {

  }

  sparkTest("sc.filterVariants should not filter out any variants when maf == 0") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val mafs = List(0.2, 0.4, 0.6, 0.8)
      .map(x => createSampleGenotypeStates(num = 5, maf = x))
      .map(x => createSampleCalledVariant(samples = Option(x)))
    val dataset = sparkSession.createDataset(mafs)

    assert(sc.filterVariants(dataset, maf = 0.0, geno = 0.0).count == 4, "sc.filterVariants filters out variants when maf == 0.0")
  }

  sparkTest("sc.filterVariants should exit gracefully when maf < 0") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List.fill(5)(createSampleCalledVariant()))
    try {
      sc.filterVariants(dataset, geno = 0.0, maf = -0.2)
      fail("sc.filterVariants does not fail on minor allele frequency < 0.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  ignore("sc.filterVariants should maf correct") {

  }

  ignore("sc.filterVariants should work correctly when an entire row is missing") {

  }

  sparkTest("sc.filterVariants should exit gracefully when geno > 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List.fill(5)(createSampleCalledVariant()))
    try {
      sc.filterVariants(dataset, geno = 1.2, maf = 0.0)
      fail("sc.filterVariants does not fail on minor allele frequency < 0.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  sparkTest("sc.filterVariants should not filter out any variants when geno == 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val genos = List(0.2, 0.4, 0.6, 0.8)
      .map(x => createSampleGenotypeStates(num = 5, geno = x))
      .map(x => createSampleCalledVariant(samples = Option(x)))
    val dataset = sparkSession.createDataset(genos)

    assert(sc.filterVariants(dataset, maf = 0.0, geno = 1.0).count == 4, "sc.filterVariants filters out variants when geno == 1.0")
  }

  sparkTest("sc.filterVariants should work properly for  0 < geno < 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val genos = List(0.2, 0.4, 0.6, 0.8)
      .map(x => createSampleGenotypeStates(num = 5, geno = x))
      .map(x => createSampleCalledVariant(samples = Option(x)))
    val dataset = sparkSession.createDataset(genos)

    assert(sc.filterVariants(dataset, geno = 0.8, maf = 0.0).count == 4, "sc.filterVariants incorrectly filtered at geno = 0.8")
    assert(sc.filterVariants(dataset, geno = 0.6, maf = 0.0).count == 3, "sc.filterVariants incorrectly filtered at geno = 0.6")
    assert(sc.filterVariants(dataset, geno = 0.4, maf = 0.0).count == 2, "sc.filterVariants incorrectly filtered at geno = 0.4")
    assert(sc.filterVariants(dataset, geno = 0.2, maf = 0.0).count == 1, "sc.filterVariants incorrectly filtered at geno = 0.2")
  }

  sparkTest("sc.filterVariants should geno _ == 0") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val genos = List(0.2, 0.4, 0.6, 0.8)
      .map(x => createSampleGenotypeStates(num = 5, geno = x))
      .map(x => createSampleCalledVariant(samples = Option(x)))
    val dataset = sparkSession.createDataset(genos)

    assert(sc.filterVariants(dataset, geno = 0.0, maf = 0.0).count == 0, "sc.filterVariants incorrectly filtered at geno = 0.0")
  }

  sparkTest("sc.filterVariants should gracefully exit when geno < 0") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(List.fill(5)(createSampleCalledVariant()))
    try {
      sc.filterVariants(dataset, geno = -0.2, maf = 0.0)
      fail("sc.filterVariants does not fail on minor allele frequency < 0.")
    } catch {
      case e: java.lang.IllegalArgumentException =>
    }
  }

  ignore("sc.filterVariants should geno correct") {

  }

  ignore("filterVariants should filter out variants with genotyping rate less than 0.1 when " +
    "geno threshold set to 0.1 and maf threshold is greater than 0.1") {

  }

  ignore("filterVariants should filter out variants with minor allele frequency less than 0.1 when" +
    "maf threshold set to 0.1 and geno threshold is greater than 0.1") {

  }

  // recode major allele tests

  sparkTest("sc.recodeMajorAllele should maf _ == 1") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val sampleVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 5, maf = 1.0)))
    val recoded = sc.recodeMajorAllele(sparkSession.createDataset(List(sampleVar))).head

    assert(recoded.chromosome == sampleVar.chromosome, "sc.recodeMajorAllele incorrectly changed the chromosome value of variants to be recoded.")
    assert(recoded.position == sampleVar.position, "sc.recodeMajorAllele incorrectly changed the position value of variants to be recoded.")
    assert(recoded.uniqueID == sampleVar.uniqueID, "sc.recodeMajorAllele incorrectly changed the uniqueID value of variants to be recoded.")
    assert(recoded.alternateAllele == sampleVar.referenceAllele, "sc.recodeMajorAllele incorrectly did not change the alternate allele value of variants to be recoded.")
    assert(recoded.referenceAllele == sampleVar.alternateAllele, "sc.recodeMajorAllele incorrectly did not change the reference allele value of variants to be recoded.")
    assert(recoded.qualityScore == sampleVar.qualityScore, "sc.recodeMajorAllele incorrectly changed the quality score value of variants to be recoded.")
    assert(recoded.filter == sampleVar.filter, "sc.recodeMajorAllele incorrectly changed the filter value of variants to be recoded.")
    assert(recoded.info == sampleVar.info, "sc.recodeMajorAllele incorrectly changed the info value of variants to be recoded.")
    assert(recoded.format == sampleVar.format, "sc.recodeMajorAllele incorrectly changed the format value of variants to be recoded.")

    val recodedSamples = sampleVar.samples.map(geno => GenotypeState(geno.sampleID, geno.toList.map {
      case "0" => "1"
      case "1" => "0"
      case "." => "."
    }.mkString("/")))

    assert(recodedSamples == recoded.samples, "sc.recodeMajorAllele incorrectly recoded the alleles in the input variant.")
  }

  sparkTest("sc.recodeMajorAllele should flip the minor and major allele when maf 1 > _ > 0.5") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val sampleVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 5, maf = 0.75)))
    val recoded = sc.recodeMajorAllele(sparkSession.createDataset(List(sampleVar))).head

    assert(recoded.chromosome == sampleVar.chromosome, "sc.recodeMajorAllele incorrectly changed the chromosome value of variants to be recoded.")
    assert(recoded.position == sampleVar.position, "sc.recodeMajorAllele incorrectly changed the position value of variants to be recoded.")
    assert(recoded.uniqueID == sampleVar.uniqueID, "sc.recodeMajorAllele incorrectly changed the uniqueID value of variants to be recoded.")
    assert(recoded.alternateAllele == sampleVar.referenceAllele, "sc.recodeMajorAllele incorrectly did not change the alternate allele value of variants to be recoded.")
    assert(recoded.referenceAllele == sampleVar.alternateAllele, "sc.recodeMajorAllele incorrectly did not change the reference allele value of variants to be recoded.")
    assert(recoded.qualityScore == sampleVar.qualityScore, "sc.recodeMajorAllele incorrectly changed the quality score value of variants to be recoded.")
    assert(recoded.filter == sampleVar.filter, "sc.recodeMajorAllele incorrectly changed the filter value of variants to be recoded.")
    assert(recoded.info == sampleVar.info, "sc.recodeMajorAllele incorrectly changed the info value of variants to be recoded.")
    assert(recoded.format == sampleVar.format, "sc.recodeMajorAllele incorrectly changed the format value of variants to be recoded.")

    val recodedSamples = sampleVar.samples.map(geno => GenotypeState(geno.sampleID, geno.toList.map {
      case "0" => "1"
      case "1" => "0"
      case "." => "."
    }.mkString("/")))

    assert(recodedSamples == recoded.samples, "sc.recodeMajorAllele incorrectly recoded the alleles in the input variant.")
  }

  sparkTest("sc.recodeMajorAllele should not recode the variant when maf == 0.5") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val sampleVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 5, maf = 0.5)))
    val recoded = sc.recodeMajorAllele(sparkSession.createDataset(List(sampleVar))).head

    assert(recoded.chromosome == sampleVar.chromosome, "sc.recodeMajorAllele incorrectly changed the chromosome value of variants to be recoded.")
    assert(recoded.position == sampleVar.position, "sc.recodeMajorAllele incorrectly changed the position value of variants to be recoded.")
    assert(recoded.uniqueID == sampleVar.uniqueID, "sc.recodeMajorAllele incorrectly changed the uniqueID value of variants to be recoded.")
    assert(recoded.referenceAllele == sampleVar.referenceAllele, "sc.recodeMajorAllele incorrectly changed the reference allele value of variants to be recoded.")
    assert(recoded.alternateAllele == sampleVar.alternateAllele, "sc.recodeMajorAllele incorrectly changed the alternate allele value of variants to be recoded.")
    assert(recoded.qualityScore == sampleVar.qualityScore, "sc.recodeMajorAllele incorrectly changed the quality score value of variants to be recoded.")
    assert(recoded.filter == sampleVar.filter, "sc.recodeMajorAllele incorrectly changed the filter value of variants to be recoded.")
    assert(recoded.info == sampleVar.info, "sc.recodeMajorAllele incorrectly changed the info value of variants to be recoded.")
    assert(recoded.format == sampleVar.format, "sc.recodeMajorAllele incorrectly changed the format value of variants to be recoded.")

    assert(sampleVar.samples == recoded.samples, "sc.recodeMajorAllele incorrectly recoded the alleles in the input variant.")
  }

  sparkTest("sc.recodeMajorAllele should not recode the variant when 0 < maf < 0.5") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val sampleVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 5, maf = 0.25)))
    val recoded = sc.recodeMajorAllele(sparkSession.createDataset(List(sampleVar))).head

    assert(recoded.chromosome == sampleVar.chromosome, "sc.recodeMajorAllele incorrectly changed the chromosome value of variants to be recoded.")
    assert(recoded.position == sampleVar.position, "sc.recodeMajorAllele incorrectly changed the position value of variants to be recoded.")
    assert(recoded.uniqueID == sampleVar.uniqueID, "sc.recodeMajorAllele incorrectly changed the uniqueID value of variants to be recoded.")
    assert(recoded.referenceAllele == sampleVar.referenceAllele, "sc.recodeMajorAllele incorrectly changed the reference allele value of variants to be recoded.")
    assert(recoded.alternateAllele == sampleVar.alternateAllele, "sc.recodeMajorAllele incorrectly changed the alternate allele value of variants to be recoded.")
    assert(recoded.qualityScore == sampleVar.qualityScore, "sc.recodeMajorAllele incorrectly changed the quality score value of variants to be recoded.")
    assert(recoded.filter == sampleVar.filter, "sc.recodeMajorAllele incorrectly changed the filter value of variants to be recoded.")
    assert(recoded.info == sampleVar.info, "sc.recodeMajorAllele incorrectly changed the info value of variants to be recoded.")
    assert(recoded.format == sampleVar.format, "sc.recodeMajorAllele incorrectly changed the format value of variants to be recoded.")

    assert(sampleVar.samples == recoded.samples, "sc.recodeMajorAllele incorrectly recoded the alleles in the input variant.")
  }

  sparkTest("sc.recodeMajorAllele should maf _ == 0") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val sampleVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 5, maf = 0.0)))
    val recoded = sc.recodeMajorAllele(sparkSession.createDataset(List(sampleVar))).head

    assert(recoded.chromosome == sampleVar.chromosome, "sc.recodeMajorAllele incorrectly changed the chromosome value of variants to be recoded.")
    assert(recoded.position == sampleVar.position, "sc.recodeMajorAllele incorrectly changed the position value of variants to be recoded.")
    assert(recoded.uniqueID == sampleVar.uniqueID, "sc.recodeMajorAllele incorrectly changed the uniqueID value of variants to be recoded.")
    assert(recoded.referenceAllele == sampleVar.referenceAllele, "sc.recodeMajorAllele incorrectly changed the reference allele value of variants to be recoded.")
    assert(recoded.alternateAllele == sampleVar.alternateAllele, "sc.recodeMajorAllele incorrectly changed the alternate allele value of variants to be recoded.")
    assert(recoded.qualityScore == sampleVar.qualityScore, "sc.recodeMajorAllele incorrectly changed the quality score value of variants to be recoded.")
    assert(recoded.filter == sampleVar.filter, "sc.recodeMajorAllele incorrectly changed the filter value of variants to be recoded.")
    assert(recoded.info == sampleVar.info, "sc.recodeMajorAllele incorrectly changed the info value of variants to be recoded.")
    assert(recoded.format == sampleVar.format, "sc.recodeMajorAllele incorrectly changed the format value of variants to be recoded.")

    assert(sampleVar.samples == recoded.samples, "sc.recodeMajorAllele incorrectly recoded the alleles in the input variant.")
  }

  sparkTest("sc.recodeMajorAllele should return `Dataset[CalledVariant]` type.") {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    val sampleVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 5, maf = 0.0)))
    val recoded = sc.recodeMajorAllele(sparkSession.createDataset(List(sampleVar)))

    assert(recoded.isInstanceOf[Dataset[CalledVariant]], "sc.recodeMajorAllele does not produce a `Dataset[CalledVariant]`")
  }

  // phenotype missing tests

  ignore("sc.loadPhenotypes should filter out phenotypes coded as -9 by default.") {

  }

  ignore("sc.loadPhenotypes should take in a list of values that should be considered as missing phenotypes.") {

  }

  ignore("sc.loadPhenotypes should filter out the entire row if a covariate is missing.") {

  }

  ignore("sc.loadPhenotypes should filter out the entire row if a primary phenotypes is missing.") {

  }

  ignore("sc.loadPhenotypes should filter out missing covariates.") {

  }
}
