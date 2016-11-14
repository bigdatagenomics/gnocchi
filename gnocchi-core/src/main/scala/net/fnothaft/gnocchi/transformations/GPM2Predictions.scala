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
//import net.fnothaft.gnocchi.models.{ GeneralizedLinearSiteModel, GenotypeState, Phenotype, Prediction }
//import org.apache.spark.SparkContext
//
//object GPM2Predictions {
//  def apply(sc: SparkContext,
//            variantId: String,
//            dataAndModel: (Array[(GenotypeState, Phenotype[Array[Double]])], GeneralizedLinearSiteModel),
//            clipOrKeepState: GenotypeState => Double): (String, Array[Prediction]) = {
//    val (data, model) = dataAndModel
//    val dataRDD = sc.parallelize(data.toList)
//    val idsAndLabeledPoints = GP2LabeledPoint(clipOrKeepState, dataRDD)
//    // predict and return prediction object
//    val preds = idsAndLabeledPoints.map(idAndData => {
//      val (sampid, lp) = idAndData
//      val pred = model.model.predict(lp.features)
//      // pack up into Prediction
//      Prediction(sampid, pred, variantId)
//    }).collect()
//    (variantId, preds)
//  }
//}