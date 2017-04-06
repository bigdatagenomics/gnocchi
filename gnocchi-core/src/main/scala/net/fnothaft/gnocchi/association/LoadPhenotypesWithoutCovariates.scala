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
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.bdgenomics.utils.misc.Logging

private[gnocchi] object LoadPhenotypesWithoutCovariates extends Serializable with Logging {

  /**
   * Loads [[Phenotype]] objects from a text file of phenotypes
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

  /**
   * Loads in and filters the phenotype values from the specified file.
   *
   * @param oneTwo if True, binary phenotypes are encoded as 1 negative response, 2 positive response
   * @param phenotypes RDD of the phenotype file read from HDFS, stored as strings
   * @param header phenotype file header string
   * @param primaryPhenoIndex index into the phenotype rows for the primary phenotype
   * @param sc spark context
   * @return RDD of [[Phenotype]] objects that contain the phenotypes from the specified file
   */
  private[gnocchi] def getAndFilterPhenotypes(oneTwo: Boolean,
                                              phenotypes: RDD[String],
                                              header: String,
                                              primaryPhenoIndex: Int,
                                              sc: SparkContext): RDD[Phenotype[Array[Double]]] = {

    // TODO: NEED TO ASSERT THAT ALL THE PHENOTYPES BE REPRESENTED BY NUMBERS.

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    var splitHeader = header.split("\t")
    val headerTabDelimited = splitHeader.length != 1
    if (!headerTabDelimited) {
      splitHeader = header.split(" ")
    }

    val indices = Array(primaryPhenoIndex)

    val data = phenotypes.map(line => if (headerTabDelimited) line.split("\t") else line.split(" "))
      .filter(p => p.length > 1)
      .filter(p => !indices.exists(index => isMissing(p(index))))
      .map(p => if (oneTwo) p.updated(primaryPhenoIndex, (p(primaryPhenoIndex).toDouble - 1).toString) else p)
      .map(p => MultipleRegressionDoublePhenotype(
        indices.map(i => splitHeader(i)).mkString(","), p(0), indices.map(i => p(i).toDouble)))
      .map(_.asInstanceOf[Phenotype[Array[Double]]])

    phenotypes.unpersist()

    data
  }

  /**
   * Checks if a phenotype value matches the missing value string
   *
   * @param value value to check
   * @return true if the value matches the missing value string, false else
   */
  private[gnocchi] def isMissing(value: String): Boolean = {
    try {
      value.toDouble == -9.0
    } catch {
      case e: java.lang.NumberFormatException => true
    }
  }
}
