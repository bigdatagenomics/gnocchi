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

case class CalledVariant(uniqueID: String,
                         chromosome: Int,
                         position: Int,
                         referenceAllele: String,
                         alternateAllele: String,
                         samples: Map[String, GenotypeState]) extends Product {

  val ploidy: Int = samples.head._2.ploidy

  /**
   * @return the minor allele frequency across all samples for this variant
   */
  def maf: Double = {
    val missingCount = samples.map(_._2.misses.toInt).sum
    val alleleCount = samples.map(_._2.alts.toInt).sum

    assert(samples.size * ploidy > missingCount, s"Variant, ${uniqueID}, has entirely missing row.")

    if (samples.size * ploidy > missingCount) {
      alleleCount.toDouble / (samples.size * ploidy - missingCount).toDouble
    } else {
      0.5
    }
  }

  /**
   * @return The fraction of missing values for this variant values across all samples
   */
  def geno: Double = {
    val missingCount = samples.map(_._2.misses.toInt).sum

    missingCount.toDouble / (samples.size * ploidy).toDouble
  }

  /**
   * @return Number of samples that have all valid values (none missing)
   */
  def numValidSamples: Int = {
    samples.count(_._2.misses == 0)
  }

  /**
   * @return Number of samples that have some valid values (could be some missing)
   */
  def numSemiValidSamples: Int = {
    samples.count(_._2.misses < ploidy)
  }
}