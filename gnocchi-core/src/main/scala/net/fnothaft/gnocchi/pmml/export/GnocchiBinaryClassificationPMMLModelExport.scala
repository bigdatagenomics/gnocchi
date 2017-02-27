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
//package org.fnothaft.gnocchi.pmml.export
//
//import scala.{ Array => SArray }
//import org.dmg.pmml._
//import org.apache.spark.mllib.regression.GeneralizedLinearModel
//
//class GnocchiBinaryClassificationPMMLModelExport(
//  model: GeneralizedLinearModel,
//  description: String,
//  variantId: String,
//  normalizationMethod: RegressionNormalizationMethodType,
//  threshold: Double)
//    extends GnocchiPMMLModelExport {
//
//  populateBinaryClassificationPMML()
//
//  /**
//   * Export the input LogisticRegressionModel or SVMModel to PMML format.
//   */
//  private def populateBinaryClassificationPMML(): Unit = {
//    pmml.getHeader.setDescription(description)
//
//    // THIS IS WHERE THE SITE-SPECIFIC DESCRIPTION IS ADDED TO THE PMML EXPORT
//    pmml.getHeader.setApplication(variantId)
//
//    if (model.weights.size > 0) {
//      val fields = new SArray[FieldName](model.weights.size)
//      val dataDictionary = new DataDictionary
//      val miningSchema = new MiningSchema
//      val regressionTableYES = new RegressionTable(model.intercept).setTargetCategory("1")
//      var interceptNO = threshold
//      if (RegressionNormalizationMethodType.LOGIT == normalizationMethod) {
//        if (threshold <= 0) {
//          interceptNO = Double.MinValue
//        } else if (threshold >= 1) {
//          interceptNO = Double.MaxValue
//        } else {
//          interceptNO = -math.log(1 / threshold - 1)
//        }
//      }
//      val regressionTableNO = new RegressionTable(interceptNO).setTargetCategory("0")
//      val regressionModel = new RegressionModel()
//        .setFunctionName(MiningFunctionType.CLASSIFICATION)
//        .setMiningSchema(miningSchema)
//        .setModelName(description)
//        .setNormalizationMethod(normalizationMethod)
//        .addRegressionTables(regressionTableYES, regressionTableNO)
//
//      for (i <- 0 until model.weights.size) {
//        fields(i) = FieldName.create("field_" + i)
//        dataDictionary.addDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
//        miningSchema
//          .addMiningFields(new MiningField(fields(i))
//            .setUsageType(FieldUsageType.ACTIVE))
//        regressionTableYES.addNumericPredictors(new NumericPredictor(fields(i), model.weights(i)))
//      }
//
//      // add target field
//      val targetField = FieldName.create("target")
//      dataDictionary
//        .addDataFields(new DataField(targetField, OpType.CATEGORICAL, DataType.STRING))
//      miningSchema
//        .addMiningFields(new MiningField(targetField)
//          .setUsageType(FieldUsageType.TARGET))
//
//      dataDictionary.setNumberOfFields(dataDictionary.getDataFields.size)
//
//      pmml.setDataDictionary(dataDictionary)
//      pmml.addModels(regressionModel)
//    }
//  }
//}