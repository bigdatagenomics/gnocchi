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
                         value: String) extends Product {
  /**
   * @note This method removes missing values from the sum, so effectively treats them as a zero value.
   * @return a sum of the genotype states stored in the value string, with missing values removed from sum.
   */
  def toDouble: Double = {
    toList.filter(_ != ".").map(_.toDouble).sum
  }

  /**
   * @note This method throws away phasing information that is stored in the value string.
   * @note This method should eventually be moved to return Doubles instead of Strings
   * @return This method splits the value field on the two delimiters.
   */
  def toList: List[String] = {
    value.split("/|\\|").toList
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
}