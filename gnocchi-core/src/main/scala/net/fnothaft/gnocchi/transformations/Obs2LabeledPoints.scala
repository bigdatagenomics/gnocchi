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
package net.fnothaft.gnocchi.transformations

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint

object Obs2LabeledPoints {

  def apply(observations: Array[(Double, Array[Double])]): Array[LabeledPoint] = {
    val points = new Array[LabeledPoint](observations.length)
    for (i <- observations.indices) {
      val elem = observations(i)
      val label = elem._2(0)
      val featuresArray = Array(1.0, elem._1) ++ elem._2.slice(1, elem._2.length)
      val features = new DenseVector(featuresArray)
      val lp = new LabeledPoint(label, features)
      points(i) = lp
    }
    points
  }
}