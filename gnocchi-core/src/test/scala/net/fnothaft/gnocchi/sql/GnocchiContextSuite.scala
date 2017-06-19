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
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import net.fnothaft.gnocchi.sql.GnocchiContext._

class GnocchiContextSuite extends GnocchiFunSuite {

  //  ignore("toGenotypeStateDataFrame should work for ploidy == 1")
  //
  //  ignore("toGenotypeStateDataFrame should work for ploidy ==2")
  //
  //  ignore("toGenotypeStateDataFrame should count number of NO_CALL's correctly")

  private def makeGenotypeState(idx: Int, sampleId: String, gs: Int, missing: Int): GenotypeState = {
    GenotypeState(s"1_${idx}_A", idx, idx + 1, "A", "C", sampleId, gs, missing)
  }

  sparkTest("filterSamples should not filter any samples if mind >= 1") {
    val allMissingSample = (1 to 10).map(makeGenotypeState(_, "sample1", 0, 2))
    val eachHalfMissingSample = (1 to 10).map(makeGenotypeState(_, "sample2", 1, 1))
    val noneMissingSample = (1 to 10).map(makeGenotypeState(_, "sample3", 2, 0))
    val halfMissingSample = (1 to 5).map(makeGenotypeState(_, "sample4", 2, 0)) ++
      (6 to 10).map(makeGenotypeState(_, "sample4", 0, 2))

    val gsCollection = allMissingSample ++ eachHalfMissingSample ++ halfMissingSample ++ noneMissingSample

    val gsRdd = sc.parallelize(gsCollection)
    val resultsRdd = sc.filterSamples(gsRdd, 1.0)

    assert(gsCollection.toSet == resultsRdd.collect.toSet, "Sample filtering did not match original collection.")
  }

  sparkTest("filterSamples should filter on mind if mind is greater than 0 but less than 1") {
    val allMissingSample = (1 to 10).map(makeGenotypeState(_, "sample1", 0, 2))
    val eachHalfMissingSample = (1 to 10).map(makeGenotypeState(_, "sample2", 1, 1))
    val noneMissingSample = (1 to 10).map(makeGenotypeState(_, "sample3", 2, 0))
    val halfMissingSample = (1 to 5).map(makeGenotypeState(_, "sample4", 2, 0)) ++
      (6 to 10).map(makeGenotypeState(_, "sample4", 0, 2))

    val badCollection = allMissingSample
    val goodCollection = eachHalfMissingSample ++ halfMissingSample ++ noneMissingSample

    val gsCollection = goodCollection ++ badCollection

    val gsRdd = sc.parallelize(gsCollection)
    val resultsRdd = sc.filterSamples(gsRdd, 0.5)

    assert(goodCollection.toSet == resultsRdd.collect.toSet, "Sample filtering did not match expected collection.")
  }

  sparkTest("filterSamples should filter out all non-perfect samples if mind == 0") {
    val allMissingSample = (1 to 10).map(makeGenotypeState(_, "sample1", 0, 2))
    val eachHalfMissingSample = (1 to 10).map(makeGenotypeState(_, "sample2", 1, 1))
    val noneMissingSample = (1 to 10).map(makeGenotypeState(_, "sample3", 2, 0))
    val halfMissingSample = (1 to 5).map(makeGenotypeState(_, "sample4", 2, 0)) ++
      (6 to 10).map(makeGenotypeState(_, "sample4", 0, 2))

    val imperfectCollection = allMissingSample ++ eachHalfMissingSample ++ halfMissingSample
    val perfectCollection = noneMissingSample

    val gsCollection = imperfectCollection ++ perfectCollection

    val gsRdd = sc.parallelize(gsCollection)
    val resultsRdd = sc.filterSamples(gsRdd, 0.0)

    assert(perfectCollection.toSet == resultsRdd.collect.toSet, "Sample filtering did not match perfect collection.")
  }

  //  ignore("filterVariants should not filter any varaints if geno and maf thresholds are both 0")
  //
  //  ignore("filterVariants should filter out variants with genotyping rate less than 0.1 when " +
  //    "geno threshold set to 0.1 and maf threshold is greater than 0.1")
  //
  //  ignore("filterVariants should filter out variants with minor allele frequency less than 0.1 when" +
  //    "maf threshold set to 0.1 and geno threshold is greater than 0.1")
  //
  //  ignore("Results from loadAndFilterGenotypes should match results of calls of filterSamples and then filterVariants")
  //
  //  ignore("loadFileAndCheckHeader should require that the input file is tab or space delimited")
  //
  //  ignore("loadFileAndCheckHeader should require that the input file has at least 2 columns")
  //
  //  ignore("loadFileAndCheckHeader should require that phenoName and each covariate match a" +
  //    "column label in the header")
  //
  //  ignore("combineAndFilterPhenotypes should filter out samples that are missing a phenotype or a covariate")
  //
  //  ignore("loadPhenotypes should call loadFileAndCheckHeader and thus catch poorly formatted headers")
  //
  //  ignore("loadPhenotypes should call combineAndFilterPhenotypes and thus produce combined phenotype objects")
  //
  //  ignore("loadPhenotypes should require that phenoName cannot match a provided covar name")
  //
  //  ignore("isMissing should yield true iff the value provided is -9.0 or -9")
  //
  //  ignore("isMissing should throw a NumberFormatException the value provided cannot be converted to a double")

}
