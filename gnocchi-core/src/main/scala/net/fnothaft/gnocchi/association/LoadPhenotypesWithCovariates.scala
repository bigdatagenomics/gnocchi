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

import htsjdk.samtools.ValidationStringency
import net.fnothaft.gnocchi.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SQLContext }
/*
Takes in a text file containing phenotypes where the first line of the textfile is a header containing the phenotype lables.
*/

private[gnocchi] object LoadPhenotypesWithCovariates extends Serializable {

  def apply[T](oneTwo: Boolean,
               file: String,
               covarFile: String,
               phenoName: String,
               covarNames: String,
               sc: SparkContext)(implicit mT: Manifest[T]): RDD[Phenotype[Array[Double]]] = {
    println("Loading phenotypes from %s.".format(file))

    // get the relevant parts of the phenotypes file and put into a DF
    val phenotypes = sc.textFile(file).persist()
    val covars = sc.textFile(covarFile).persist()
    println("Loading covars form %s.".format(covarFile))

    // separate header and data
    val header = phenotypes.first()
    val covarHeader = covars.first()

    // get label indices
    val covarLen = covarHeader.split("\t").length
    var covarLabels = Array(("", 0))
    if (covarLen >= 2) {
      covarLabels = covarHeader.split("\t").zipWithIndex
    } else {
      covarLabels = covarHeader.split(" ").zipWithIndex
    }
    assert(covarLabels.length >= 2, "Covars file must have a minimum of 2 tab delimited columns. The first being some form of sampleID, the rest being covar values. A header with column labels must also be present. ")
    val len = header.split("\t").length
    var labels = Array(("", 0))
    if (len >= 2) {
      labels = header.split("\t").zipWithIndex
    } else {
      labels = header.split(" ").zipWithIndex
    }

    assert(labels.length >= 2, "Phenotypes file must have a minimum of 2 tab delimited columns. The first being some form of sampleID, the rest being phenotype values. A header with column labels must also be present. ")

    // extract covarNames
    val covariates = covarNames.split(",")

    // get the index of the phenotype to be regressed
    var primaryPhenoIndex = -1
    var phenoMatch = false
    for (labelpair <- labels) {
      if (labelpair._1 == phenoName) {
        phenoMatch = true
        primaryPhenoIndex = labelpair._2 // this should give you an option, and then you check to see if the option exists. 
      }
    }
    assert(phenoMatch, "The phenoName given doesn't match any of the phenotypes specified in the header.")

    // get the indices of the covariates
    val covarIndices = new Array[Int](covariates.length)
    var i = 0
    for (covar <- covariates) {
      var hasMatch = false
      if (covar == phenoName) {
        assert(false, "One or more of the covariates has the same name as phenoName.")
      }
      for (labelpair <- covarLabels) {
        if (labelpair._1 == covar) {
          hasMatch = true
          covarIndices(i) = labelpair._2
          i = i + 1
        }
      }
      assert(hasMatch, "One or more of the names from covarNames doesn't match a column title in the header of the phenotype file.")
    }

    // construct the phenotypes RDD, filtering out all samples that don't have the phenotype or one of the covariates
    val data = getAndFilterPhenotypes(oneTwo, phenotypes, covars, header, covarHeader, primaryPhenoIndex, covarIndices, sc)

    return data
  }

  private[gnocchi] def getAndFilterPhenotypes(oneTwo: Boolean,
                                              phenotypes: RDD[String],
                                              covars: RDD[String],
                                              header: String,
                                              covarHeader: String,
                                              primaryPhenoIndex: Int,
                                              covarIndices: Array[Int],
                                              sc: SparkContext): RDD[Phenotype[Array[Double]]] = {

    // !!! NEED TO ASSERT THAT ALL THE PHENOTPES BE REPRESENTED BY NUMBERS.

    // initialize sqlContext
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    // split up the header for making the phenotype label later
    var splitHeader = header.split("\t")
    val headerTabDelimited = splitHeader.length != 1
    //    println("HeaderTab = " + headerTabDelimited)
    if (!headerTabDelimited) {
      splitHeader = header.split(" ")
    }
    var splitCovarHeader = covarHeader.split("\t")
    val covarTabDelimited = splitCovarHeader.length != 1
    //    println("covarTab = " + covarTabDelimited)
    if (!covarTabDelimited) {
      splitCovarHeader = covarHeader.split(" ")
    }
    val fullHeader = splitHeader ++ splitCovarHeader
    val numInPheno = splitHeader.length
    //    println("numInPheno: " + numInPheno)
    //    println("covarIndices: " + covarIndices.toList)
    //    println("\n\n\n\n covarIndices = " + covarIndices.toList + "\n\n\n\n\n")
    val mergedIndices = covarIndices.map(elem => { elem + numInPheno })

    // construct the RDD of Phenotype objects from the data in the textfile
    //    println("\n\n\n\n primary Pheno Index = " + primaryPhenoIndex + "\n\n\n\n\n")
    val indices = Array(primaryPhenoIndex) ++ mergedIndices
    //    println(" \n\n\n\n\n\n indices: " + indices.toList + "\n\n\n\n\n ")
    //    println("indices: " + indices.toList)
    var covarData = covars.filter(line => line != covarHeader)
      .map(line => line.split(" ")).keyBy(splitLine => splitLine(0)).filter(_._1 != "")
    if (covarTabDelimited) {
      covarData = covars.filter(line => line != covarHeader)
        .map(line => line.split("\t")).keyBy(splitLine => splitLine(0)).filter(_._1 != "")
    }

    var data = phenotypes.filter(line => line != header)
      // split the line by column
      .map(line => line.split(" ")).keyBy(splitLine => splitLine(0)).filter(_._1 != "")
    if (headerTabDelimited) {
      data = phenotypes.filter(line => line != header)
        // split the line by column
        .map(line => line.split("\t")).keyBy(splitLine => splitLine(0)).filter(_._1 != "")
    }
    //    covarData.take(10).map(d => {
    //      println("sampleId: " + d._1)
    //      println("covars: " + d._2.toList)
    //    })
    //    data.take(10).map(d => {
    //      println("sampleId: " + d._1)
    //      println("data: " + d._2.toList)
    //    })
    val dataToPrint = data.take(5).toList
    println(dataToPrint)
    // merge the phenos and covariates into same RDD row

    // TODO: took out the cogroup here. Need to fix.

    val joinedData = data.cogroup(covarData).map(pair => {
      val (sampleId, (phenos, covariates)) = pair
      val phenoArray = phenos.toArray
      val covarArray = covariates.toArray
      //      println(phenoArray.length)
      //      println(covarArray.length)
      val toret = phenoArray(0) ++ covarArray(0)
      //      println(toret.length)
      //      println(toret)
      toret
    })
    // filter out empty lines and samples missing the phenotype being regressed. Missing values denoted by -9.0

    val finalData = joinedData.filter(p => {
      //      println("p: " + p.toList)
      //      println("plength = " + p.length)
      if (p.length > 2) {

        var keep = true
        for (valueIndex <- indices) {
          //          println("index = " + valueIndex)
          //          println(p.toList)
          //          println("here: " + p(valueIndex).toDouble)
          if (isMissing(p(valueIndex))) {
            keep = false
          }
        }
        keep
      } else {
        false
      }
    }).map(p => {
      if (oneTwo) {
        println("oneTwo flagged")
        val toRet = p.slice(0, primaryPhenoIndex) ++ List((p(primaryPhenoIndex).toDouble - 1).toString) ++ p.slice(primaryPhenoIndex + 1, p.length)
        println("toRet: " + toRet.toList)
        toRet
      } else {
        p
      }
    })
      // construct a phenotype object from the data in the sample
      .map(p => new MultipleRegressionDoublePhenotype(
        (for (i <- indices) yield fullHeader(i)).mkString(","), // phenotype labels string
        p(0), // sampleID string
        for (i <- indices) yield p(i).toDouble) // phenotype values
        .asInstanceOf[Phenotype[Array[Double]]])
    //    println("\n\n\n\n\n\n\n\n" + fullHeader.toList + "\n\n\n\n\n\n")
    //    println("\n\n\n\n\n\n\n\n" + indices.toList + "\n\n\n\n\n")
    // unpersist the textfile
    phenotypes.unpersist()
    covars.unpersist()

    finalData
  }

  private[gnocchi] def isMissing(value: String): Boolean = {
    try {
      value.toDouble == -9.0
    } catch {
      case e: java.lang.NumberFormatException => true
    }
  }
}
