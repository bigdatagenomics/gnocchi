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

case class GenotypeState(sampleID: String,
                         refs: Int,
                         alts: Int,
                         misses: Int) extends Product {

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
    if (toDouble == 0.0) 0.0 else 1.0
  }

  def recessive: Double = {
    if (toDouble == 2.0) 1.0 else 0.0
  }

  def ploidy: Int = {
    refs + alts + misses
  }
}

object GenotypeState {
  /**
   * Static method to support legacy GenotypeState instantiation, with slash-separated integer values
   * @return a GenotypeState with values derived from gsStr
   */
  def apply(sampleID: String, gsStr: String): GenotypeState = {
    val alleleLst = gsStr.split("/|\\|")
    val altsum = alleleLst.filter(x => x != "0" && x != ".").map(_.toDouble).sum
    GenotypeState(sampleID, alleleLst.count(_ == "0"), alleleLst.count(_ == "1"), alleleLst.count(_ == "."))
  }
}