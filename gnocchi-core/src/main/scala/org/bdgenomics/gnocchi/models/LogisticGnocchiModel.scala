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
package org.bdgenomics.gnocchi.models

import org.apache.spark.sql.Dataset
import org.bdgenomics.gnocchi.models.variant.LogisticVariantModel
import org.bdgenomics.gnocchi.utils.ModelType._

/**
 * Data container for [[LogisticVariantModel]] and the associated metadata that is shared between
 * the independent variant models in an association study.
 *
 * @param variantModels Dataset of [[LogisticVariantModel]], each of which store the variant level
 *                      statistical model for the study
 * @param phenotypeName The names of the primary phenotype for the study being conducted. This
 *                      phenotype acts as the label or Y of a sample when building each variant model.
 * @param covariatesNames The names of the covariates used in for the study being conducted. These
 *                        variable are the variables that are being controlled for, when building each
 *                        variant model
 * @param sampleUIDs The unique identifiers for the subjects used in this study, to build this model
 * @param numSamples the number of subjects used in this study, to build this model
 * @param allelicAssumption The allelic assumption used in this study, to build this model
 */
case class LogisticGnocchiModel(@transient variantModels: Dataset[LogisticVariantModel],
                                phenotypeName: String,
                                covariatesNames: List[String],
                                sampleUIDs: Set[String],
                                numSamples: Int,
                                allelicAssumption: String)
    extends GnocchiModel[LogisticVariantModel, LogisticGnocchiModel] {
  val modelType: ModelType = Logistic
}