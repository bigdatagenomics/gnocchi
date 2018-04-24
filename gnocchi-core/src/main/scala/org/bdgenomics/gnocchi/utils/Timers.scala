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
package org.bdgenomics.gnocchi.utils

import org.bdgenomics.utils.instrumentation.Metrics

object Timers extends Metrics {

  // Filter methods
  val FilterSamples = timer("Filter Samples")
  val FilterVariants = timer("Filter Variants")
  val RecodeMajorAllele = timer("Recode Major Allele")

  // Load methods
  val LoadGenotypes = timer("Load Genotypes")
  val LoadGnocchiGenotypes = timer("Load Gnocchi Formatted Genotypes")
  val LoadCalledVariantDSFromVariantContextRDD = timer("Load Gnocchi Format from ADAM Format")
  val LoadPhenotypes = timer("Load Phenotypes")
  val LoadGnocchiModel = timer("Load in a Gnocchi Model")

  // Save Methods
  val SaveAssociations = timer("Save Associations")

  // Regression Methods
  val CreateModelAndAssociations = timer("Create Model and Associations")

  val CreatAssociationsDataset = timer("Create Associations Dataset")
}
