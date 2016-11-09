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

import net.fnothaft.gnocchi.models.{ GenotypeState, Phenotype, Association }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
import org.apache.spark.mllib.regression.LabeledPoint
import net.fnothaft.gnocchi.transformations.GP2LabeledPoint

trait BuildVariantModel {

  def apply[T](observations: Array[(Double, Array[Double])],
               locus: ReferenceRegion,
               altAllele: String,
               phenotype: String): VariantModel[T] = {

    // call RegressPhenotypes on the data
    val assoc = compute(observations, locus, altAllele, phenotype)

    // extract the model parameters (including p-value) for the variant and build VariantModel
    extractVariantModel(assoc)

  }

  def compute(observations: Array[(Double, Array[Double])],
              locus: ReferenceRegion,
              altAllele: String,
              phenotype: String): Association

  def extractVariantModel(assoc: Association): VariantModel

}

trait AdditiveVariant {
  protected def clipOrKeepState(observations: Array[(Double, Array[Double])): Double = {
    observations.map(obs => {
      (obs._1.toDouble, obs._2)
    }
  }
}

trait DominantVariant {
  protected def clipOrKeepState(observations: Array[(Double, Array[Double])): Double = {
    observations.pam(obs => {
      if (obs._1 == 0) (0.0, obs._2) else (1.0, obs._2)
    }
  }
}


