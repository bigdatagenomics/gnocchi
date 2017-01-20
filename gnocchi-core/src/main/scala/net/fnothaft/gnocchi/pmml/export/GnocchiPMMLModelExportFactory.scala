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
//package net.fnothaft.gnocchi.pmml.export
//
//import org.apache.spark.mllib.classification.LogisticRegressionModel
//import org.apache.spark.mllib.classification.SVMModel
//import org.apache.spark.mllib.clustering.KMeansModel
//import org.apache.spark.mllib.regression.LassoModel
//import org.apache.spark.mllib.regression.LinearRegressionModel
//import org.apache.spark.mllib.regression.RidgeRegressionModel
//
//object GnocchiPMMLModelExportFactory {
//  def createPMMLModelExport(model: Any, variantDescription: String): GnocchiPMMLModelExport = {
//    model match {
//      // TODO: implement kmeans
//      // case kmeans: KMeansModel =>
//      //   new GnocchiKMeansPMMLModelExport(kmeans)
//      // TODO: implement lin reg
//      // case linear: LinearRegressionModel =>
//      //   new GnocchiGeneralizedLinearPMMLModelExport(linear, "linear regression")
//      // TODO: implement Ridge reg
//      // case ridge: RidgeRegressionModel =>
//      //   new GnocchiGeneralizedLinearPMMLModelExport(ridge, "ridge regression")
//      // TODO: implement lasso
//      // case lasso: LassoModel =>
//      //   new GnocchiGeneralizedLinearPMMLModelExport(lasso, "lasso regression")
//      case svm: SVMModel =>
//        new GnocchiBinaryClassificationPMMLModelExport(
//          svm, "linear SVM", variantId, RegressionNormalizationMethodType.NONE,
//          svm.getThreshold.getOrElse(0.0))
//      // TODO: implement logistic reg
//      // case logistic: LogisticRegressionModel =>
//      //   if (logistic.numClasses == 2) {
//      //     new GnocchiBinaryClassificationPMMLModelExport(
//      //       logistic, "logistic regression", RegressionNormalizationMethodType.LOGIT,
//      //       logistic.getThreshold.getOrElse(0.5))
//      //   } else {
//      //     throw new IllegalArgumentException(
//      //       "PMML Export not supported for Multinomial Logistic Regression")
//      // }
//      case _ =>
//        throw new IllegalArgumentException(
//          "GnocchiPMML Export not supported for model: " + model.getClass.getName)
//    }
//  }
//}