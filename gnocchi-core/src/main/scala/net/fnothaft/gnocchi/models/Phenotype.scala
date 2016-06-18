/**
 * Copyright 2015 Frank Austin Nothaft
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
package net.fnothaft.gnocchi.models

trait Phenotype[T] extends Product {
  val phenotype: String
  val sampleId: String
  val value: T

  def toDouble: Double
}

case class IntPhenotype(phenotype: String,
                        sampleId: String,
                        value: Int) extends Phenotype[Int] {

  def toDouble: Double = value.toDouble
}

case class DoublePhenotype(phenotype: String,
                           sampleId: String,
                           value: Double) extends Phenotype[Double] {

  def toDouble: Double = value
}

case class BooleanPhenotype(phenotype: String,
                            sampleId: String,
                            value: Boolean) extends Phenotype[Boolean] {

  def toDouble: Double = if (value) 1.0 else 0.0
}
