///**
//  * Copyright 2016 Taner Dagdelen
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//*/
//
//package net.fnothaft.gnocchi.transformations
//
//import net.fnothaft.gnocchi.models.{GeneralizedLinearSiteModel, GenotypeState, Phenotype, TestResult}
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
//object GPM2TestResult {
//  def apply(sc: SparkContext,
//            variantId: String,
//            dataAndModel:(Array[(GenotypeState, Phenotype[Array[Double]])], GeneralizedLinearSiteModel),
//            clipOrKeepState: GenotypeState => Double): TestResult = {
//    // make predictions RDD[(String, Arra[Predictions])]
//    val (variantId, preds) = GPM2Predictions(sc, variantId, dataAndModel, clipOrKeepState)
//    val predsRDD = sc.parallelize(preds).map(prediction => (prediction.sampleId, prediction.predictedY)
//    val (data, model) = dataAndModel
//    val actualsRDD = sc.parallelize(data).map(sample => {
//      val (g, p) = sample
//      (g.sampleId, p.value(0))
//    })
//    TestResult(variantId, mse(predsRDD,actualsRDD))
//  }
//
//  def mse(predsRDD: RDD[(String, Double)],
//          actualsRDD: RDD[(String, Double)]): Double = {
//    // (sampleId, pred), (sampleId, actual) -> (sampleId, (Iterable[pred], Iterable[actual]))
//    val sErrors = predsRDD.cogroup(actualsRDD)
//    // calculate the squared error
//    .map(sample=> {
//      val (id, (pred, act)) = sample
//      Math.pow(pred.toList(0)-act.toList(0), 2)
//    })
//    val num = sErrors.count()
//    sErrors.sum()/num
//  }
//}