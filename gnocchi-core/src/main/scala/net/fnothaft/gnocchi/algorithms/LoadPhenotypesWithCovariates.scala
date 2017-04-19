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
package net.fnothaft.gnocchi.algorithms

import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.Logging

private[gnocchi] object LoadPhenotypesWithCovariates extends Serializable with Logging {

  /**
   *  Loads [[Phenotype]] objects from text files of phenotypes and covariates
   *
   * @param oneTwo Phenotype response classification encoded as 1 null response, 2 positive response
   * @param file File path to phenotype file
   * @param covarFile file path to covariates file
   * @param phenoName name of the primary phenotype
   * @param covarNames name of the covariates to include
   * @return RDD of [[Phenotype]] objects
   */
  def apply[T](oneTwo: Boolean,
               file: String,
               covarFile: String,
               phenoName: String,
               covarNames: String,
               sc: SparkContext)(implicit mT: Manifest[T]): RDD[Phenotype] = {
    logInfo("Loading phenotypes from %s.".format(file))
    val phenotypes = sc.textFile(file).persist()

    logInfo("Loading covars form %s.".format(covarFile))
    val covars = sc.textFile(covarFile).persist()

    val header = phenotypes.first()
    val covarHeader = covars.first()

    val len = header.split("\t").length
    var labels = Array(("", 0))
    if (len >= 2) {
      labels = header.split("\t").zipWithIndex
    } else {
      labels = header.split(" ").zipWithIndex
    }

    require(labels.length >= 2,
      "Phenotypes file must have a minimum of 2 tab delimited columns. The first being some " +
        "form of sampleID, the rest being phenotype values. A header with column labels must also be present. ")

    val covarLen = covarHeader.split("\t").length
    var covarLabels = Array(("", 0))
    if (covarLen >= 2) {
      covarLabels = covarHeader.split("\t").zipWithIndex
    } else {
      covarLabels = covarHeader.split(" ").zipWithIndex
    }

    require(covarLabels.length >= 2,
      "Covars file must have a minimum of 2 tab delimited columns. The first being some " +
        "form of sampleID, the rest being covar values. A header with column labels must also be present. ")

    val covariates = covarNames.split(",")
    require(!covariates.contains(phenoName), "One or more of the covariates has the same name as phenoName.")

    val primaryPhenoIndex = labels.map(item => item._1).indexOf(phenoName)
    require(primaryPhenoIndex != -1, "The phenoName given doesn't match any of the phenotypes specified in the header.")

    val indices = covarLabels.filter(item => covariates.contains(item._1)).map(x => x._2)
    require(indices.length == covariates.length,
      "One or more of the names from covarNames doesn't match a column title in the header of the phenotype file.")

    val covarIndices = new Array[Int](covariates.length)
    indices.copyToArray(covarIndices)

    getAndFilterPhenotypes(oneTwo, phenotypes, covars, header, covarHeader, primaryPhenoIndex, covarIndices, sc)
  }

  /**
   * Loads in and filters the phenotype and covariate values from the specified files.
   *
   * @param oneTwo if True, binary phenotypes are encoded as 1 negative response, 2 positive response
   * @param phenotypes RDD of the phenotype file read from HDFS, stored as strings
   * @param covars RDD of the covars file read from HDFS, stored as strings
   * @param header phenotype file header string
   * @param covarHeader covar file header string
   * @param primaryPhenoIndex index into the phenotype rows for the primary phenotype
   * @param covarIndices indices of the covariates in the covariate rows
   * @param sc spark context
   * @return RDD of [[Phenotype]] objects that contain the phenotypes and covariates from the specified files
   */
  private[gnocchi] def getAndFilterPhenotypes(oneTwo: Boolean,
                                              phenotypes: RDD[String],
                                              covars: RDD[String],
                                              header: String,
                                              covarHeader: String,
                                              primaryPhenoIndex: Int,
                                              covarIndices: Array[Int],
                                              sc: SparkContext): RDD[Phenotype] = {

    // TODO: NEED TO REQUIRE THAT ALL THE PHENOTYPES BE REPRESENTED BY NUMBERS.

    var splitHeader = header.split("\t")
    val headerTabDelimited = splitHeader.length != 1
    if (!headerTabDelimited) {
      splitHeader = header.split(" ")
    }

    var splitCovarHeader = covarHeader.split("\t")
    val covarTabDelimited = splitCovarHeader.length != 1
    if (!covarTabDelimited) {
      splitCovarHeader = covarHeader.split(" ")
    }

    val fullHeader = splitHeader ++ splitCovarHeader
    val mergedIndices = covarIndices.map(elem => { elem + splitHeader.length })
    val indices = Array(primaryPhenoIndex) ++ mergedIndices

    val data = phenotypes.filter(line => line != header)
      .map(line => if (headerTabDelimited) line.split("\t") else line.split(" "))
      .keyBy(splitLine => splitLine(0))
      .filter(_._1 != "")

    val covarData = covars.filter(line => line != covarHeader)
      .map(line => if (covarTabDelimited) line.split("\t") else line.split(" "))
      .keyBy(splitLine => splitLine(0))
      .filter(_._1 != "")

    val joinedData = data.cogroup(covarData).map(pair => {
      val (_, (phenosIterable, covariatesIterable)) = pair
      val phenoArray = phenosIterable.toList.head
      val covarArray = covariatesIterable.toList.head
      phenoArray ++ covarArray
    })

    val finalData = joinedData
      .filter(p => p.length > 2)
      .filter(p => !indices.exists(index => isMissing(p(index))))
      .map(p => if (oneTwo) p.updated(primaryPhenoIndex, (p(primaryPhenoIndex).toDouble - 1).toString) else p)
      .map(p => Phenotype(
        indices.map(index => fullHeader(index)).mkString(","), p(0), indices.map(i => p(i).toDouble)))
      .map(_.asInstanceOf[Phenotype])

    phenotypes.unpersist()
    covars.unpersist()

    finalData
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
