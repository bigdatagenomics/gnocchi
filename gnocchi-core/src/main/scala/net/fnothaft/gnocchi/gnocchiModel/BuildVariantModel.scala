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
package net.fnothaft.gnocchi.gnocchiModel

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, VariantModel }
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Variant

trait BuildVariantModel {

  def apply[T](observations: Array[(Double, Array[Double])],
               variant: Variant,
               phenotype: String): VariantModel = {

    // call RegressPhenotypes on the data
    val assoc = compute(observations, variant, phenotype)

    // extract the model parameters (including p-value) for the variant and build VariantModel
    extractVariantModel(assoc)

  }

  def compute(observations: Array[(Double, Array[Double])],
              variant: Variant,
              phenotype: String): Association

  def extractVariantModel(assoc: Association): VariantModel

}

trait AdditiveVariant extends BuildVariantModel {
  def arrayClipOrKeepState(observations: Array[(Double, Array[Double])]): Array[(Double, Array[Double])] = {
    observations.map(obs => {
      (obs._1.toDouble, obs._2)
    })
  }

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    gs.genotypeState.toDouble
  }
}

trait DominantVariant extends BuildVariantModel {
  def arrayClipOrKeepState(observations: Array[(Double, Array[Double])]): Array[(Double, Array[Double])] = {
    observations.map(obs => {
      if (obs._1 == 0) (0.0, obs._2) else (1.0, obs._2)
    })
  }

  protected def clipOrKeepState(gs: GenotypeState): Double = {
    if (gs.genotypeState == 0) 0.0 else 1.0
  }
}

