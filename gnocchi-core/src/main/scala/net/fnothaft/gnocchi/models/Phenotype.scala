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
package net.fnothaft.gnocchi.models

trait Phenotype[T] extends Product {
  val phenotype: String
  val sampleId: String
  val value: T

  def toDouble: Array[Double]
}

/**
 * Implementation of [[Phenotype]] that formalizes multiple regression with covariates.
 *
 * @param phenotype Line that contains the names of all the phenotypes (primary and covariates), separated by spaces
 * @param sampleId Individual's sample id
 * @param value Array of all phenotypes being used. First item is primary phenotype, the rest are covariates
 */
case class MultipleRegressionDoublePhenotype(phenotype: String,
                                             sampleId: String,
                                             value: Array[Double]) extends Phenotype[Array[Double]] {
  def toDouble: Array[Double] = value
}
