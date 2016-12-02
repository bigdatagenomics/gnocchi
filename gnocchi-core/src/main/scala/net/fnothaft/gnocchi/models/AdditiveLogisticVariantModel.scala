/**
 * Copyright 2016 Taner Dagdelen
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

import net.fnothaft.gnocchi.association.{Additive, LogisticSiteRegression}
import org.bdgenomics.formats.avro.Variant

class AdditiveLogisticVariantModel extends LogisticVariantModel with Additive with LogisticSiteRegression {

//  variantID: String,
//  numSamples: Int,
//  variance: Double,
//  hyperParamValues: Map[String, Double],
//  weights: Array[Double],
//  haplotypeBlock: String,
//  incrementalUpdateValue: Double, QRFactorizationValue: Double)

  modelType = "Additive Logistic Variant Model"
  variance = 0.0
  variantID = "No ID for this Variant"
  variant = new Variant
  hyperParamValues = Map[String, Double]()
  weights = Array[Double]()
  haplotypeBlock = "Nonexistent HaplotypeBlock"
  incrementalUpdateValue = 0.0

}
