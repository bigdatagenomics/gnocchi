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
//package org.fnothaft.gnocchi.pmml
//
//import java.io.{ File, OutputStream, StringWriter }
//import javax.xml.transform.stream.StreamResult
//import org.jpmml.model.JAXBUtil
//import org.apache.spark.SparkContext
//import org.apache.spark.annotation.{ DeveloperApi, Since }
//import org.apache.spark.mllib.pmml.export.PMMLModelExportFactory
//
//trait GnocchiPMMLExportable {
//
//  private def toGnocchiPMML(streamResult: StreamResult, variantId: String): Unit = {
//    val pmmlModelExport = GnocchiPMMLModelExportFactory.createPMMLModelExport(this, variantId)
//    JAXBUtil.marshalPMML(pmmlModelExport.getPmml, streamResult)
//  }
//
//  def toGnocchiPMML(localPath: String, variantId: String): Unit = {
//    toGnocchiPMML(new StreamResult(new File(localPath)), variantId)
//  }
//
//  def toGnocchiPMML(sc: SparkContext, path: String, variantId: String): Unit = {
//    val pmml = toGnocchiPMML(variantId)
//    sc.parallelize(Array(pmml), 1).saveAsTextFile(path)
//  }
//
//  def toGnocchiPMML(outputStream: OutputStream, variantId: String): Unit = {
//    toPMML(new StreamResult(outputStream), variantId)
//  }
//
//  def toGnocchiPMML(variantId: String): String = {
//    val writer = new StringWriter
//    toGnocchiPMML(new StreamResult(writer), variantId)
//    writer.toString
//  }
//
//}