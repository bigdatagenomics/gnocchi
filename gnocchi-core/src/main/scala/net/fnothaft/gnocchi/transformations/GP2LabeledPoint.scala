///**
// * Copyright 2016 Taner Dagdelen
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package net.fnothaft.gnocchi.transformations
//
//import net.fnothaft.gnocchi.models.{ GenotypeState, Phenotype }
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.rdd.RDD
//import org.apache.spark.mllib.linalg.DenseVector
//
//object GP2LabeledPoint {
//
//  def apply(clipOrKeepState: GenotypeState => Double,
//            siteData: RDD[(GenotypeState, Phenotype[Array[Double]])]): RDD[(String, LabeledPoint)] = {
//    // transform the data into design vector (x) and label (y)
//    siteData.map(sample => {
//      val (genoState, phenotype) = sample
//      val label = phenotype.value(0)
//      clipOrKeepState(genoState)
//      val featureList = List[Double](genoState.genotypeState) ::: phenotype.value.drop(1).toList
//      val features = new DenseVector(featureList.toArray)
//      (genoState.sampleId, new LabeledPoint(label, features))
//    })
//  }
//}