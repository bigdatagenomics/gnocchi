/**
 * Copyright 2016 Frank Austin Nothaft
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
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

trait ValidationRegression extends SiteRegression {

  /*
  Takes in an RDD of GenotypeStates, constructs the proper observations array for each site, and feeds it into
  regressSite
  */
  final def apply[T](rdd: RDD[GenotypeState],
                     phenotypes: RDD[Phenotype[T]],
                     scOption: Option[SparkContext] = None,
                     k: Int = 1,
                     n: Int = 1,
                     monte: Boolean = false): Array[RDD[(Array[(String, (Double, Double))], Association)]] = {
    val genoPhenoRdd = rdd.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))
    val progressiveResults = new Array[RDD[(Array[(String, (Double, Double))], Association)]](n)

    if (k != 1) {
      if (monte) {
        if (n != 1) {
          // random [1/n] split kfolds times.
          // Split genotype array into equal pieces of size 1/n
          var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
          // Incrementally build up training set by merging first two elements (training set) and testing on second element
          val trainRdd = splitArray(0)
          val testRdd = splitArray(1)
          progressiveResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
          for (a <- 1 until n) {
            splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
            splitArray.drop(1)
            val trainRdd = splitArray(0)
            val testRdd = splitArray(1)
            progressiveResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
          }
        } else {
          // a single k-1/1 split kfolds times.
          val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(1.0 - (1.0 / k), 1.0 / k))
          applyRegression(trainRdd, testRdd, phenotypes)
        }
      } else {
        if (n != 1) {
          assert(false, "Normal crossvalidation not possible for progressive validation. Please use --monteCarlo.")
        } else {
          // kfold splits with rotating train/test. Note: ValidationRegression should only be called ONCE.
          var splitArray = genoPhenoRdd.randomSplit(Array.fill(k)(1f / k))
          for (a <- 1 until k) {
            val (trainRdd, testRdd) = mergeRDDs(a, splitArray)
            progressiveResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
          }
        }
      }
    } else {
      if (n != 1) {
        // 1 random [1/n] split
        // Split array genotype array into equal pieces of size 1/n
        var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
        // Incrementally build up training set by merging first two elements (training set) and testing on second element
        val trainRdd = splitArray(0)
        val testRdd = splitArray(1)
        progressiveResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
        for (a <- 1 until n) {
          splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
          splitArray.drop(1)
          val trainRdd = splitArray(0)
          val testRdd = splitArray(1)
          progressiveResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
        }
      } else {
        // 1 random 90/10 split
        val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(.9, .1))
        applyRegression(trainRdd, testRdd, phenotypes)
      }
    }
    if (n == 1) {
      val Array(trainRdd, testRdd) = genoPhenoRdd.randomSplit(Array(1.0 - (1.0 / k), 1.0 / k))
      applyRegression(trainRdd, testRdd, phenotypes)
    } else {
      // Split array genotype array into equal pieces of size 1/n
      var splitArray = genoPhenoRdd.randomSplit(Array.fill(n)(1f / n))
      // Incrementally build up training set by merging first two elements (training set) and testing on second element
      val trainRdd = splitArray(0)
      val testRdd = splitArray(1)
      progressiveResults(0) = applyRegression(trainRdd, testRdd, phenotypes)
      for (a <- 1 until n) {
        splitArray(1) = splitArray(1).join(splitArray(0)).flatMapValues(x => List(x._1))
        splitArray.drop(1)
        val trainRdd = splitArray(0)
        val testRdd = splitArray(1)
        progressiveResults(a) = applyRegression(trainRdd, testRdd, phenotypes)
      }
    }
    progressiveResults
  }

  def mergeRDDs[T](exclude: Int, rddArray: Array[RDD[(String, (GenotypeState, Phenotype[T]))]]): (RDD[(String, (GenotypeState, Phenotype[T]))], RDD[(String, (GenotypeState, Phenotype[T]))]) = {
    var first = true
    var testRdd: RDD[(String, (GenotypeState, Phenotype[T]))] = rddArray(0)
    var trainRdd: RDD[(String, (GenotypeState, Phenotype[T]))] = rddArray(0)
    for (i <- rddArray.indices) {
      if (i == exclude) {
        val trainRDD = rddArray(i)
      } else {
        if (first) {
          testRdd = rddArray(i)
          first = false
        } else {
          testRdd = testRdd.join(rddArray(i)).flatMapValues(x => List(x._1))
        }
      }
    }
    (testRdd, trainRdd)
  }

  def applyRegression[T](trainRdd: RDD[(String, (GenotypeState, Phenotype[T]))],
                         testRdd: RDD[(String, (GenotypeState, Phenotype[T]))],
                         phenotypes: RDD[Phenotype[T]]): RDD[(Array[(String, (Double, Double))], Association)] = {
    val modelRdd = trainRdd
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (_, p) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = p
        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = new Variant()
        val contig = new Contig()
        contig.setContigName(gs.contig)
        variant.setContig(contig)
        variant.setStart(gs.start)
        variant.setEnd(gs.end)
        variant.setAlternateAllele(gs.alt)
        ((variant, pheno.phenotype), p)
      }).groupByKey()
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
      }).filter(varModel => {
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
        val variant = new Variant()
        val contig = new Contig()
        contig.setContigName(gs.contig)
        variant.setContig(contig)
        variant.setStart(gs.start)
        variant.setEnd(gs.end)
        variant.setAlternateAllele(gs.alt)
        ((variant, pheno.phenotype), (sampleid, p))
      }).groupByKey()
    //    println("\n\n" + temp.take(1).toList)
    println("pre-join samples at a site: \n" + temp.take(5).toList)
    val temp2 = temp.join(modelRdd)
    println("Post-join samples and models at a site: \n" + temp2.take(0).toList)
    println(temp2.take(1).toList)
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

