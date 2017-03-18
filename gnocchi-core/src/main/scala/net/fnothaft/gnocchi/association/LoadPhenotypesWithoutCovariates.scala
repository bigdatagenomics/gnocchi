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

import htsjdk.samtools.ValidationStringency
import net.fnothaft.gnocchi.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SQLContext }
import org.bdgenomics.utils.misc.Logging

/*
Takes in a text file containing phenotypes where the first line of the textfile is a header containing the phenotype lables.
*/

private[gnocchi] object LoadPhenotypesWithoutCovariates extends Serializable with Logging {

  /**
   * Loads [[Phenotype]] objects from a text file
   *
   * @param oneTwo Phenotype response classification encoded as 1 null response, 2 positive response
   * @param file File path to phenotype file
   * @param phenoName Name of the primary Phenotype
   * @return RDD containing the [[Phenotype]] objects loaded from the specified phenotype file
   */
  def apply[T](oneTwo: Boolean,
               file: String,
               phenoName: String,
               sc: SparkContext)(implicit mT: Manifest[T]): RDD[Phenotype[Array[Double]]] = {
    logInfo("Loading phenotypes from %s.".format(file))

    val phenotypes = sc.textFile(file).persist()
    val header = phenotypes.first()

    val len = header.split("\t").length
    var labels = Array(("", 0))
    if (len >= 2) {
      labels = header.split("\t").zipWithIndex
    } else {
      labels = header.split(" ").zipWithIndex
    }

    require(labels.length >= 2,
      "Phenotypes file must have a minimum of 2 tab delimited columns. The first being some" +
        " form of sampleID, the rest being phenotype values. A header with column labels must also be present. ")

    val primaryPhenoIndex = labels.map(item => item._1).indexOf(phenoName)
    require(primaryPhenoIndex != -1, "The phenoName given doesn't match any of the phenotypes specified in the header.")

    getAndFilterPhenotypes(oneTwo, phenotypes, header, primaryPhenoIndex, sc)
  }

  private[gnocchi] def getAndFilterPhenotypes(oneTwo: Boolean,
                                              phenotypes: RDD[String],
                                              header: String,
                                              primaryPhenoIndex: Int,
                                              sc: SparkContext): RDD[Phenotype[Array[Double]]] = {

    // TODO: NEED TO ASSERT THAT ALL THE PHENOTPES BE REPRESENTED BY NUMBERS.

    // initialize sqlContext
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    // split up the header for making the phenotype label later
    var splitHeader = header.split("\t")
    val headerTabDelimited = splitHeader.length != 1
    if (!headerTabDelimited) {
      splitHeader = header.split(" ")
    }

    // construct the RDD of Phenotype objects from the data in the textfile
    val indices = Array(primaryPhenoIndex)
    val data = phenotypes.filter(line => line != header)
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
      }).map(p => {
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

    return data
  }

  private[gnocchi] def isMissing(value: String): Boolean = {
    try {
      value.toDouble == -9.0
    } catch {
      case e: java.lang.NumberFormatException => true
    }
  }
}
