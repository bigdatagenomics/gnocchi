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

  def apply(ensembleMethod: String, snpArray: Array[(Double, Double, Association)]): (Double, Double) = {
    ensembleMethod match {
      case "AVG" => average(snpArray)
      case _     => average(snpArray) //still call avg until other methods implemented
    }
  }

  def average(snpArray: Array[(Double, Double, Association)]): (Double, Double) = {
    var sm = 0.0
    for (i <- snpArray.indices) {
      sm += snpArray(i)._1
    }
    (sm / snpArray.length, snpArray(0)._2)
  }
}