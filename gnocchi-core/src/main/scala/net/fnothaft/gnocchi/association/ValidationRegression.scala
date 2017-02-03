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
import net.fnothaft.gnocchi.transformations.Gs2variant
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait ValidationRegression extends SiteRegression {

  /*
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into
  regressSite
  */
  def apply[T](rdd: RDD[GenotypeState],
               phenotypes: RDD[Phenotype[T]],
               scOption: Option[SparkContext] = None,
               k: Int = 1,
               n: Int = 1,
               sc: SparkContext,
               monte: Boolean = false): Array[RDD[(Array[(String, (Double, Double))], Association)]] = {
    val genoPhenoRdd = rdd.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    val crossValResults = new Array[RDD[(Array[(String, (Double, Double))], Association)]](k)

    // 1 random 90/10 split
    println("\n\n\n\n\n\n\n k=1, n=1 \n\n\n\n\n\n\n")
    val rdds = genoPhenoRdd.randomSplit(Array(.9, .1))
    val trainRdd = rdds(0)
    val testRdd = rdds(1)
    crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)

    //    if (k != 1) {
    //      if (monte) {
    //        if (n != 1) {
    //          // random [1/n] split kfolds times.
    //          // Split genotype array into equal pieces of size 1/n
    //          var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
    //          // Incrementally build up training set by merging first two elements (training set) and testing on second element
    //          val trainRdd = splitArray(0)
    //          val testRdd = splitArray(1)
    //          println("\n\n\n\n\n\n In apply, trainRdd count: " + trainRdd.count)
    //          println("SplitArray length: " + splitArray.length)
    //          crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
    //          for (a <- 1 until n) {
    //            splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
    //            splitArray.drop(1)
    //            val trainRdd = splitArray(0)
    //            val testRdd = splitArray(1)
    //            crossValResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
    //          }
    //        } else {
    //          // a single k-1/1 split kfolds times.
    //          val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(1.0 - (1.0 / k), 1.0 / k))
    //          crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
    //        }
    //      } else {
    //        if (n != 1) {
    //          assert(false, "kfolds cross validation not possible for progressive validation. Please use --monteCarlo.")
    //        } else {
    //          // kfold splits with rotating train/test. Note: ValidationRegression should only be called ONCE.
    //          var splitArray = genoPhenoRdd.randomSplit(Array.fill(k)(1f / k))
    //          //          println("\n\n\n\n\n\n number of splits: " + splitArray.length + "\n\n\n\n\n")
    //          for (a <- splitArray.indices) {
    //            println(s"\n\n\n\n\n\n\n a = $a \n\n\n\n\n\n\n\n\n")
    //            println("\n\n\n\n\n\n number of splits: " + splitArray.length + "\n\n\n\n\n")
    //            val (testRdd, trainRdd) = mergeRDDs(sc, a, splitArray)
    //            println("\n\n\n\n\n\n testRdd length: " + testRdd.count)
    //            println("\n\n\n\n\n\n trainRdd length: " + trainRdd.count)
    //            crossValResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
    //          }
    //        }
    //      }
    //    } else {
    //      if (n != 1) {
    //        // 1 random [1/n] split
    //        // Split array genotype array into equal pieces of size 1/n
    //        var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
    //        // Incrementally build up training set by merging first two elements (training set) and testing on second element
    //        val trainRdd = splitArray(0)
    //        val testRdd = splitArray(1)
    //        println("\n\n\n\n\n\n In apply, trainRdd count: " + trainRdd.count)
    //        println("SplitArray length: " + splitArray.length)
    //        crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
    //        for (a <- 1 until n) {
    //          splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
    //          splitArray.drop(1)
    //          val trainRdd = splitArray(0)
    //          val testRdd = splitArray(1)
    //          crossValResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
    //        }
    //      } else {
    //        // 1 random 90/10 split
    //        println("\n\n\n\n\n\n\n k=1, n=1 \n\n\n\n\n\n\n")
    //        val rdds = genoPhenoRdd.randomSplit(Array(.9, .1))
    //        val trainRdd = rdds(0)
    //        val testRdd = rdds(1)
    //        crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
    //      }
    //    }
    //    if (n == 1) {
    //      val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(1.0 - (1.0 / k), 1.0 / k))
    //      applyRegression(trainRdd, testRdd, phenotypes)
    //    } else {
    //      // Split array genotype array into equal pieces of size 1/n
    //      var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
    //      // Incrementally build up training set by merging first two elements (training set) and testing on second element
    //      val trainRdd = splitArray(0)
    //      val testRdd = splitArray(1)
    //      crossValResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
    //      for (a <- 1 until n) {
    //        splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
    //        splitArray.drop(1)
    //        val trainRdd = splitArray(0)
    //        val testRdd = splitArray(1)
    //        crossValResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
    //      }
    //    }
    crossValResults
  }

  //  def mergeRDDs[T](sc: SparkContext, exclude: Int, rddArray: Array[RDD[(String, (GenotypeState, Phenotype[T]))]]): (RDD[(String, (GenotypeState, Phenotype[T]))], RDD[(String, (GenotypeState, Phenotype[T]))]) = {
  //    var testRdd: RDD[(String, (GenotypeState, Phenotype[T]))] = rddArray(0)
  //    var trainingData: Array[(String, (GenotypeState, Phenotype[T]))] = new Array[(String, (GenotypeState, Phenotype[T]))](0)
  //    if (exclude == 0) {
  //      trainingData = rddArray(1).collect
  //    } else {
  //      trainingData = rddArray(0).collect
  //    }
  //    for (i <- rddArray.indices) {
  //      if (i == exclude) {
  //        val testRdd = rddArray(i)
  //      } else {
  //        println("pre-join count: " + trainingData.length) // 67 samples in pre-join count
  //        //          trainRdd = trainRdd.join(rddArray(i)).flatMapValues(x => List(x._1))
  //        trainingData = trainingData ++ rddArray(i).collect
  //        println("post-join count: " + trainingData.length) // 0 sampels in post-join count.
  //      }
  //    }
  //    val trainRdd = sc.parallelize(trainingData)
  //    (testRdd, trainRdd)
  //  }

  def applyRegression[T](trainRdd: RDD[(String, (GenotypeState, Phenotype[T]))],
                         testRdd: RDD[(String, (GenotypeState, Phenotype[T]))],
                         phenotypes: RDD[Phenotype[T]]): RDD[(Array[(String, (Double, Double))], Association)] = {
    println("TrainRDD count: " + trainRdd.count)
    val temp01 = trainRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = Gs2variant(gs)
        ((variant, pheno.phenotype), p)
      })
    println("temp01: \n" + temp01.count + "\n" + temp01.take(5).toList)
    val temp0 = temp01.groupByKey()
      .map(site => {
        val ((variant, pheno), observations) = site

        // build array to regress on, and then regress
        val assoc = regressSite(observations.map(p => {
          // unpack p
          val (genotypeState, phenotype) = p
          // return genotype and phenotype in the correct form
          (clipOrKeepState(genotypeState), phenotype.toDouble)
        }).toArray, variant, pheno)
        ((variant, pheno), assoc)
      })
    println("\n\n\n\n\n\n\n temp0 count: " + temp0.count + "\n\n\n\n\n\n\n\n\n")
    val modelRdd = temp0.filter(varModel => {
      val ((variant, phenotype), assoc) = varModel
      assoc.statistics.nonEmpty
    })
    //    println("\n\n" + modelRdd.take(1).toList)

    val temp = testRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (sampleid, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = Gs2variant(gs)
        ((variant, pheno.phenotype), (sampleid, p))
      }).groupByKey()
    //    println("\n\n" + temp.take(1).toList)
    val temp2 = temp.join(modelRdd)
    temp2.map(site => {
      val (key, value) = site
      val (sampleObservations, association) = value
      val (variant, phenotype) = key

      (predictSite(sampleObservations.map(p => {
        // unpack p
        val (sampleid, (genotypeState, phenotype)) = p
        // return genotype and phenotype in the correct form
        (clipOrKeepState(genotypeState), phenotype.toDouble, sampleid)
      }).toArray, association), association)
    })
  }

  /**
   * Method to predict phenotype on a site.
   *
   * Predicts the phenotype given the genotype and covariates, after regression.
   * To be implemented by any class that implements this trait.
   */
  protected def predictSite(sampleObservations: Array[(Double, Array[Double], String)],
                            association: Association): Array[(String, (Double, Double))]
}

