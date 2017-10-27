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

case class CalledVariant(chromosome: Int,
                         position: Int,
                         uniqueID: String,
                         referenceAllele: String,
                         alternateAllele: String,
                         qualityScore: String,
                         filter: String,
                         info: String,
                         format: String,
                         samples: List[GenotypeState]) extends Product {

  /**
   * @return the minor allele frequency across all samples for this variant
   */
  def maf: Double = {
    val ploidy = samples.head.toList.length
    val sampleValues: List[String] = samples.flatMap(_.toList)
    val missingCount = sampleValues.count(_ == ".")
    val alleleCount = sampleValues.filter(_ != ".").map(_.toDouble).sum

    // assert(sampleValues.length > missingCount, s"Variant, ${uniqueID}, has entirely missing row. Fix by filtering variants with geno = 1.0")

    if (sampleValues.length > missingCount) {
      alleleCount / (sampleValues.length - missingCount)
    } else {
      0.5
    }
  }

  /**
   * @return The fraction of missing values for this variant values across all samples
   */
  def geno: Double = {
    val ploidy = samples.head.toList.length
    val sampleValues: List[String] = samples.flatMap(_.toList)
    val missingCount = sampleValues.count(_ == ".")

    missingCount.toDouble / sampleValues.length.toDouble
  }

  /**
   * @return Number of samples that have all valid values (none missing)
   */
  def numValidSamples: Int = {
    samples.count(x => !x.value.contains("."))
  }

  /**
   * @return Number of samples that have some valid values (could be some missing)
   */
  def numSemiValidSamples: Int = {
    samples.count(x => x.toList.count(_ != ".") > 0)
  }
}