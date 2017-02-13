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

private[gnocchi] object LoadPhenotypesWithoutCovariates extends Serializable {

  def apply[T](oneTwo: Boolean,
               file: String,
               phenoName: String,
               sc: SparkContext,
               writeMissingPheno: String = null)(implicit mT: Manifest[T]): RDD[Phenotype[Array[Double]]] = {
    println("Loading phenotypes from %s.".format(file))

    // get the relevant parts of the phenotypes file and put into a DF
    val phenotypes = sc.textFile(file).persist()

    // separate header and data
    val header = phenotypes.first()

    // get label indices
    val len = header.split("\t").length
    var labels = Array(("", 0))
    if (len >= 2) {
      labels = header.split("\t").zipWithIndex
    } else {
      labels = header.split(" ").zipWithIndex
    }
    //    val labels = header.split("\t").zipWithIndex

    assert(labels.length >= 2, "Phenotypes file must have a minimum of 2 tab delimited columns. The first being some form of sampleID, the rest being phenotype values. A header with column labels must also be present. ")

    // get the index of the phenotype to be regressed
    var primaryPhenoIndex = -1
    var phenoMatch = false
    for (labelpair <- labels) {
      if (labelpair._1 == phenoName) {
        phenoMatch = true
        primaryPhenoIndex = labelpair._2 // this should give you an option, and then you check to see if the option exists. 
      }
    }
    assert(phenoMatch, "The phenoName given doesn't match any of the phenotypes specified in the header of the phenotypes textfile.")

    // construct the phenotypes dataset, filtering out all samples that don't have the phenotype or one of the covariates
    val data = getAndFilterPhenotypes(oneTwo, phenotypes, header, primaryPhenoIndex, sc, writeMissingPheno)

    // """Should be able to store the data in a more readable format as well."""
    return data
  }

  private[gnocchi] def getAndFilterPhenotypes(oneTwo: Boolean,
                                              phenotypes: RDD[String],
                                              header: String,
                                              primaryPhenoIndex: Int,
                                              sc: SparkContext,
                                              writeMissingPheno: String = null): RDD[Phenotype[Array[Double]]] = {

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

    // construct the RDD of Phenotype objects from the data in the textfile
    val indices = Array(primaryPhenoIndex)
    if (writeMissingPheno != null) {
      val dirtyData = phenotypes.filter(line => line != header)
        // split the line by column
        .map(line => line.split("\t"))
        // filter out empty lines and samples missing the phenotype being regressed. Missing values denoted by -9.0
        .filter(p => {
          if (p.length > 1) {
            var keep = false
            for (valueIndex <- indices) {
              if (isMissing(p(valueIndex))) {
                keep = true
              }
            }
            keep
          } else {
            true
          }
        })
        .coalesce(1)
        .saveAsTextFile(writeMissingPheno)
    }

    val cleanData = phenotypes.filter(line => line != header)
      // split the line by column
      .map(line => line.split("\t"))
      // filter out empty lines and samples missing the phenotype being regressed. Missing values denoted by -9.0
      .filter(p => {
        if (p.length > 1) {
          var keep = true
          for (valueIndex <- indices) {
            if (isMissing(p(valueIndex))) {
              keep = false
            }
          }
          keep
        } else {
          false
        }
      })
      .map(p => {
        if (oneTwo) {
          p.slice(0, primaryPhenoIndex) ++ List((p(primaryPhenoIndex).toDouble - 1).toString) ++ p.slice(primaryPhenoIndex + 1, p.length)
        } else {
          p
        }
      })
      // construct a phenotype object from the data in the sample
      .map(p => new MultipleRegressionDoublePhenotype(
        (for (i <- indices) yield splitHeader(i)).mkString(","), // phenotype labels string
        p(0), // sampleID string
        for (i <- indices) yield p(i).toDouble) // phenotype values
        .asInstanceOf[Phenotype[Array[Double]]])

    // unpersist the textfile
    phenotypes.unpersist()

    return cleanData
  }

  private[gnocchi] def isMissing(value: String): Boolean = {
    try {
      value.toDouble == -9.0
    } catch {
      case e: java.lang.NumberFormatException => true
    }
  }
}
