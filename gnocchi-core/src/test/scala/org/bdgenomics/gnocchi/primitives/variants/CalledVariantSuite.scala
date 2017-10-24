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
package org.bdgenomics.gnocchi.primitives.variants

import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.GnocchiFunSuite

class CalledVariantSuite extends GnocchiFunSuite {

  // minor allele frequency tests
  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: minor allele is actually the major allele") {
    // create a called variant object where the alt has a frequency of 70%, maf should return 0.7
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.7, geno = 0)))
    assert(calledVar.maf == 0.7, "CalledVariant.maf does not calculate Minor Allele frequency correctly when the specified alternate allele has a higher frequency than the reference allele.")
  }

  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: no minor alleles present.") {
    // create a called variant object where there are no alts.
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0, geno = 0)))
    assert(calledVar.maf == 0.0, "CalledVariant.maf does not calculate Minor Allele frequency correctly when there are no minor alleles present.")
  }

  sparkTest("CalledVariant.maf should return the frequency rate of the minor allele: only minor alleles present.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 1.0, geno = 0)))
    assert(calledVar.maf == 1.0, "CalledVariant.maf does not calculate Minor Allele frequency correctly when there are no minor alleles present.")
  }

  ignore("CalledVariant.maf should return the frequency rate of the minor allele: all values are missing.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.0, geno = 1.0)))
    try {
      calledVar.maf
      fail("CalledVariant.maf should throw an assertion error when there are all missing values.")
    } catch {
      case e: java.lang.AssertionError =>
    }
  }

  // genotyping rate tests

  sparkTest("CalledVariant.geno should return the fraction of missing values: all values are missing.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.0, geno = 1.0)))
    assert(calledVar.geno == 1.0, "CalledVariant.geno incorrectly calculates genotyping missingness when all values are missing.")
  }

  sparkTest("CalledVariant.geno should return the fraction of missing values: no values are missing.") {
    val calledVar = createSampleCalledVariant(samples = Option(createSampleGenotypeStates(num = 100, maf = 0.0, geno = 0.0)))
    assert(calledVar.geno == 0.0, "CalledVariant.geno incorrectly calculates genotyping missingness when nos values are missing.")
  }

  // num valid samples tests

  sparkTest("CalledVariant.numValidSamples should only count samples with no missing values") {
    val iids = random.shuffle(1000 to 9999).take(20)
    val genos = List.fill(5)("./.") ++ List.fill(5)("./1") ++ List.fill(10)("1/1")
    val genosStates = genos.zip(iids).map(x => GenotypeState(x._2.toString, x._1))

    val calledVariant = createSampleCalledVariant(samples = Option(genosStates))
    assert(calledVariant.numValidSamples == 10, "Number of valid samples miscounted.")
  }

  // num semi valid samples tests

  sparkTest("CalledVariant.numSemiValidSamples should count the number of samples that have some valid values.") {
    val iids = random.shuffle(1000 to 9999).take(20)
    val genos = List.fill(5)("./.") ++ List.fill(5)("./1") ++ List.fill(10)("1/1")
    val genosStates = genos.zip(iids).map(x => GenotypeState(x._2.toString, x._1))

    val calledVariant = createSampleCalledVariant(samples = Option(genosStates))
    assert(calledVariant.numSemiValidSamples == 15, "Number of valid samples miscounted.")
  }
}
