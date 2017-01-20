/**
 * Copyright 2015 Frank Austin Nothaft and Taner Dagdelen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.Association

object Ensembler extends Serializable {

  /**
   * Ensembles association results.
   *
   * @param ensembleMethod method by which to ensemble the results. Choose from ["AVG", "MAX_PROB", "W_AVG"].
   * @param snpArray array of tup3 objects containing (predicted result, actual result, Association object).
   * @param weights array of weights to use for each snp in the snp array. Ordered the same as the snpArray param.
   */
  def apply(ensembleMethod: String, snpArray: Array[(Double, Double, Association)], weights: Array[Double] = Array.empty[Double]) = {
    ensembleMethod match {
      case "AVG"      => average(snpArray)
      case "MAX_PROB" => maxProb(snpArray)
      case "W_AVG"    => weightedAvg(snpArray, weights)
      case _          => average(snpArray) //still call avg until other methods implemented
    }
  }

  /* Average the predicted values of all values in the snpArray */
  def average(snpArray: Array[(Double, Double, Association)]): (Double, Double) = {
    var sm = 0.0
    for (i <- snpArray.indices) {
      sm += snpArray(i)._1
    }
    (sm / snpArray.length, snpArray(0)._2)
  }

  /* Take the maximum of predicted values from all values in the snpArray */
  def maxProb(snpArray: Array[(Double, Double, Association)]): (Double, Double) = {
    var max = 0.0
    for (i <- snpArray.indices) {
      if (snpArray(i)._1 > max) {
        max = snpArray(i)._1
      }
    }
    (max, snpArray(0)._2)
  }

  /* Weighted average the predicted values of all values in the snpArray */
  def weightedAvg(snpArray: Array[(Double, Double, Association)], weights: Array[Double]): (Double, Double) = {
    assert(snpArray.length == weights.length, "Array length mismatch in weighted average ensembler.")
    var sm = 0.0

    var norm_weights = weights.clone()
    var sum = 0.0
    var i = 0
    while (i < weights.length) { sum += weights(i); i += 1 }
    i = 0
    while (i < norm_weights.length) { norm_weights(i) = (norm_weights(i) / sum); i += 1 }

    for (i <- snpArray.indices) {
      sm += norm_weights(i) * snpArray(i)._1
    }
    (sm, snpArray(0)._2)
  }
}