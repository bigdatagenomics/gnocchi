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
package org.bdgenomics.gnocchi.primitives.genotype

/**
 * Data storage primitive for a single subject's genotypic variant information at a single location
 *
 * @param refs count of reference alleles at this location
 * @param alts count of alternate alleles at this location
 * @param misses count of misses at this location
 */
case class GenotypeState(refs: Byte,
                         alts: Byte,
                         misses: Byte) extends Product {
  /**
   * @note This method removes missing values from the sum, so effectively treats them as a zero value.
   * @return a sum of the genotype states stored in the value string, with missing values removed from sum.
   */
  def toDouble: Double = {
    alts.toDouble
  }

  def additive: Double = {
    toDouble
  }

  def dominant: Double = {
    if (alts >= 1.0) 1.0 else 0.0
  }

  def recessive: Double = {
    if (toDouble == 2.0) 1.0 else 0.0
  }

  def ploidy: Int = {
    refs + alts + misses
  }
}