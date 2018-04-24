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
package org.bdgenomics.gnocchi.sql

import org.apache.spark.broadcast.Broadcast
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype

/**
 * Container object for the distributed [[Phenotype]] [[Map]]. Stores the [[Phenotype]] objects
 * as well as the primary phenotype's name and the covariate names.
 *
 * @param phenotypes [[Phenotype]] [[Map]] that stores the SampleID --> [[Phenotype]]
 * @param phenotypeName Primary phenotype name
 * @param covariateNames Covariate names
 */
case class PhenotypesContainer(phenotypes: Broadcast[Map[String, Phenotype]],
                               phenotypeName: String,
                               covariateNames: Option[List[String]]) {
  val numSamples: Int = phenotypes.value.size
}