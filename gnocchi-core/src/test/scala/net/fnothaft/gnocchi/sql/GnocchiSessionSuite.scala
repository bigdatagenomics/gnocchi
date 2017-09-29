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

package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.algorithms.siteregression.{ AdditiveLinearRegression, DominantLinearRegression }
import net.fnothaft.gnocchi.primitives.genotype.GenotypeState
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import net.fnothaft.gnocchi.sql.GnocchiSession._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.{ Dataset, SparkSession }

import scala.util.Random

class GnocchiSessionSuite extends GnocchiFunSuite {

  // load Genotypes tests

  sparkTest("sc.loadGenotypes should produce a dataset of CalledVariant objects.") {
    val genoPath = testFile("small1.vcf")
    val genotypes = sc.loadGenotypes(genoPath)
    assert(genotypes.isInstanceOf[Dataset[CalledVariant]], "LoadGenotypes should produce a" +
      " Dataset[CalledVariant]")
  }

  sparkTest("sc.loadGenotypes should map fields correctly.") {
    val genoPath = testFile("1Sample1Variant.vcf")
    val genotypes = sc.loadGenotypes(genoPath)
    val firstCalledVariant = genotypes.orderBy("position").head

    assert(firstCalledVariant.uniqueID.equals("rs3131972"))
    assert(firstCalledVariant.chromosome === 1)
    assert(firstCalledVariant.position === 752721)
    assert(firstCalledVariant.referenceAllele === "A")
    assert(firstCalledVariant.alternateAllele === "G")
    assert(firstCalledVariant.samples.equals(List(GenotypeState("sample1", "0/1"))))
  }

  ignore("sc.loadGenotypes should gracefully exit when a non-existing file path is passed in.") {

  }

  ignore("sc.loadGenotypes should have no overlapping values in the `uniqueID` field.") {

  }

  ignore("sc.loadGenotypes should be able to take in ADAM formatted parquet files with genotype states.") {

  }

  // load phenotypes tests

  ignore("sc.loadPhenotypes should gracefully exit when a non-existing file path is passed in.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when the specified primaryID is not a column in the phenotypes file.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when passed an invalid phenotype delimiter.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when passed an invalid covariate delimiter.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when the primary phenotype is listed as a covariate.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when a covariates path is passed in without covariate names.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when covariate names are passed in without a covariates path.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when a covariate name is not a column in the covariate file.") {

  }

  ignore("sc.loadPhenotypes should gracefully exit when primary phenotype is not a column in the phenotype file.") {

  }

  ignore("sc.loadPhenotypes should produce a scala `Map[String, Phenotype]`.") {

  }

  ignore("sc.loadPhenotypes should properly load in covariates.") {

  }

  ignore("sc.loadPhenotypes should create empty lists in the covariate field of the Phenotype objects if there are no covariates.") {

  }

  ignore("sc.loadPhenotypes should filter out samples with missing phenotype values.") {

  }

  ignore("sc.loadPhenotypes should correctly match covariates to phenotypes based off of primaryID.") {

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

  ignore("sc.filterSamples should not allow for mind < 0.") {

  }

  ignore("sc.filterSamples should remove samples from return dataset if they do not have a phenotype.") {

  }

  // filter variants tests

  ignore("sc.filterVariants should maf _ > 1") {

  }

  ignore("sc.filterVariants should maf _ == 1") {

  }

  ignore("sc.filterVariants should maf 1 > _ > 0") {

  }

  ignore("sc.filterVariants should maf _ == 0") {

  }

  ignore("sc.filterVariants should maf _ < 0") {

  }

  ignore("sc.filterVariants should maf correct") {

  }

  ignore("sc.filterVariants should geno _ > 1") {

  }

  ignore("sc.filterVariants should geno _ == 1") {

  }

  ignore("sc.filterVariants should geno 1 > _ > 0") {

  }

  ignore("sc.filterVariants should geno _ == 0") {

  }

  ignore("sc.filterVariants should geno _ < 0") {

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

  ignore("sc.recodeMajorAllele should maf _ > 1") {

  }

  ignore("sc.recodeMajorAllele should maf _ == 1") {

  }

  ignore("sc.recodeMajorAllele should maf 1 > _ > 0.5") {

  }

  ignore("sc.recodeMajorAllele should maf  _ == 0.5") {

  }

  ignore("sc.recodeMajorAllele should maf 0.5 > _ > 0") {

  }

  ignore("sc.recodeMajorAllele should maf _ == 0") {

  }

  ignore("sc.recodeMajorAllele should maf _ < 0") {

  }

  ignore("sc.recodeMajorAllele should return `Dataset[CalledVariant]` type.") {

  }

  // phenotype missing tests

  ignore("isMissing should yield true iff the value provided is -9.0 or -9") {

  }

  ignore("isMissing should throw a NumberFormatException the value provided cannot be converted to a double") {

  }
}