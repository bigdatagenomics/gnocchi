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
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, Phenotype }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait Progressive extends ValidationRegression {
  /*
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into
  regressSite
  */
  override def apply[T](rdd: RDD[GenotypeState],
                        phenotypes: RDD[Phenotype[T]],
                        scOption: Option[SparkContext] = None,
                        k: Int = 1,
                        n: Int = 1,
                        sc: SparkContext,
                        monte: Boolean = false): Array[RDD[(Array[(String, (Double, Double))], Association)]] = {
    val genoPhenoRdd = rdd.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    val crossValResults = new Array[RDD[(Array[(String, (Double, Double))], Association)]](k)

    // random [1/n] split 
    // Split genotype array into equal pieces of size 1/n
    var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
    // Incrementally build up training set by merging first two elements (training set) and testing on second element
    val trainRdd = splitArray(0)
    val testRdd = splitArray(1)
    println("\n\n\n\n\n\n In apply, trainRdd count: " + trainRdd.count)
    println("SplitArray length: " + splitArray.length)
    crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
    for (a <- 1 until n) {
      splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
      splitArray.drop(1)
      val trainRdd = splitArray(0)
      val testRdd = splitArray(1)
      crossValResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
    }
    crossValResults
  }
}