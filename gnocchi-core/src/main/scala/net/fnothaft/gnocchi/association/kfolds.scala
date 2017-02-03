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
package net.fnothaft.gnocchi.association

import net.fnothaft.gnocchi.models.{ Association, GenotypeState, Phenotype }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait kfolds extends ValidationRegression {
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

    // kfold splits with rotating train/test. Note: ValidationRegression should only be called ONCE.
    var splitArray = genoPhenoRdd.randomSplit(Array.fill(k)(1f / k))
    //          println("\n\n\n\n\n\n number of splits: " + splitArray.length + "\n\n\n\n\n")
    for (a <- splitArray.indices) {
      println(s"\n\n\n\n\n\n\n a = $a \n\n\n\n\n\n\n\n\n")
      println("\n\n\n\n\n\n number of splits: " + splitArray.length + "\n\n\n\n\n")
      val (testRdd, trainRdd) = mergeRDDs(sc, a, splitArray)
      println("\n\n\n\n\n\n testRdd length: " + testRdd.count)
      println("\n\n\n\n\n\n trainRdd length: " + trainRdd.count)
      crossValResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
    }
    crossValResults
  }

  def mergeRDDs[T](sc: SparkContext, exclude: Int, rddArray: Array[RDD[(String, (GenotypeState, Phenotype[T]))]]): (RDD[(String, (GenotypeState, Phenotype[T]))], RDD[(String, (GenotypeState, Phenotype[T]))]) = {
    var testRdd: RDD[(String, (GenotypeState, Phenotype[T]))] = rddArray(0)
    var trainingData: Array[(String, (GenotypeState, Phenotype[T]))] = new Array[(String, (GenotypeState, Phenotype[T]))](0)
    if (exclude == 0) {
      trainingData = rddArray(1).collect
    } else {
      trainingData = rddArray(0).collect
    }
    for (i <- rddArray.indices) {
      if (i == exclude) {
        val testRdd = rddArray(i)
      } else {
        println("pre-join count: " + trainingData.length) // 67 samples in pre-join count
        //          trainRdd = trainRdd.join(rddArray(i)).flatMapValues(x => List(x._1))
        trainingData = trainingData ++ rddArray(i).collect
        println("post-join count: " + trainingData.length) // 0 sampels in post-join count.
      }
    }
    val trainRdd = sc.parallelize(trainingData)
    (testRdd, trainRdd)
  }
}