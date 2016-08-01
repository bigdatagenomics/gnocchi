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
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, Row, SQLContext }
/*
Should be able to load the relevant phenotypes from a textfile where the first line is a header. 

Need to 1) Load only the relevant columns of the textfile
        2) Convert the file into a Dataset[Phenotype[Array[T]]
        3) Count [Optional] and cut out any samples with missing phenotypes. 
        4) Count [Optional] and cut out any samples with missing covariates. 
        5) Return the resulting Dataset[PHenotype[Array[T]]]
*/

private[gnocchi] object LoadPhenotypesWithCovariates extends Serializable with Logging {

  def apply[T](file: String,
               phenoName: String,
               covarNames: String,
               sc: SparkContext)(implicit mT: Manifest[T]): RDD[Phenotype[Array[Double]]] = {
    log.info("Loading phenotypes from %s.".format(file))

    // get the relevant parts of the phenotypes file and put into a DF
    val phenotypes = sc.textFile(file).persist()

    // separate header and data
    val header = phenotypes.first()

    // get label indices
    val labels = header.split("\t").zipWithIndex
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
      for (labelpair <- labels) {
        if (labelpair._1 == covar) {
          hasMatch = true
          covarIndices(i) = labelpair._2
          i = i + 1
        }
      }
      assert(hasMatch, "One or more of the names from covarNames doesn't match a column title in the header of the phenotype file.")
    }

    // construct the phenotypes RDD, filtering out all samples that don't have the phenotype or one of the covariates
    val data = getAndFilterPhenotypes(phenotypes, header, primaryPhenoIndex, covarIndices, sc)

    return data
  }

  private[gnocchi] def getAndFilterPhenotypes(phenotypes: RDD[String],
                                              header: String,
                                              primaryPhenoIndex: Int,
                                              covarIndices: Array[Int],
                                              sc: SparkContext): RDD[Phenotype[Array[Double]]] = {

    // !!! NEED TO ASSERT THAT ALL THE PHENOTPES BE REPRESENTED BY NUMBERS.

    // initialize sqlContext
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    // split up the header for making the phenotype label later
    val splitHeader = header.split("\t")

    // construct the RDD of Phenotype objects from the data in the textfile
    val indices = Array(primaryPhenoIndex) ++ covarIndices
    val data = phenotypes.filter(line => line != header)
      // split the line by column
      .map(line => line.split("\t"))
      // filter out samples missing the phenotype being regressed.
      .filter(p => {
        var keep = true
        for (valueIndex <- indices) {
          if (isMissing(p(valueIndex))) {
            keep = false
          }
        }
        keep
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
    value.toDouble == -9.0
  }
}
