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
package net.fnothaft.gnocchi.sql

import java.io.File

import net.fnothaft.gnocchi.models.{ GnocchiModel, GnocchiModelMetaData }
import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
import net.fnothaft.gnocchi.models.variant.VariantModel
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.utils.misc.Logging
import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import net.fnothaft.gnocchi.rdd.association._
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.bdgenomics.utils.cli._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ concat, lit }
import net.fnothaft.gnocchi.rdd.genotype.GenotypeState
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype
import org.apache.hadoop.fs.Path
import org.bdgenomics.adam.cli.Vcf2ADAM
import java.io._
import java.util

object GnocchiContext {

  // Add GnocchiContext methods
  implicit def sparkContextToGnocchiContext(sc: SparkContext): GnocchiContext =
    new GnocchiContext(sc)

}

class GnocchiContext(@transient val sc: SparkContext) extends Serializable with Logging {

  // sets up sparkSession
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def toGenotypeStateDataset(gtFrame: DataFrame, ploidy: Int): Dataset[GenotypeState] = {
    toGenotypeStateDataFrame(gtFrame, ploidy).as[GenotypeState]
  }

  def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int, sparse: Boolean = false): DataFrame = {

    val filteredGtFrame = if (sparse) {
      // if we want the sparse representation, we prefilter
      val sparseFilter = (0 until ploidy).map(i => {
        gtFrame("alleles").getItem(i) =!= "Ref"
      }).reduce(_ || _)
      gtFrame.filter(sparseFilter)
    } else {
      gtFrame
    }

    // generate expression
    val genotypeState = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "REF", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val missingGenotypes = (0 until ploidy).map(i => {
      val c: Column = when(filteredGtFrame("alleles").getItem(i) === "NO_CALL", 1).otherwise(0)
      c
    }).reduce(_ + _)

    val phaseSetId: Column = when(filteredGtFrame("phaseSetId").isNull, 0).otherwise(filteredGtFrame("phaseSetId"))

    filteredGtFrame.select(filteredGtFrame("variant.contigName").as("contigName"),
      filteredGtFrame("variant.start").as("start"),
      filteredGtFrame("variant.end").as("end"),
      filteredGtFrame("variant.referenceAllele").as("ref"),
      filteredGtFrame("variant.alternateAllele").as("alt"),
      filteredGtFrame("sampleId"),
      genotypeState.as("genotypeState"),
      missingGenotypes.as("missingGenotypes"),
      phaseSetId.as("phaseSetId"))
  }

  def loadAndFilterGenotypes(genotypesPath: String,
                             adamDestination: String,
                             ploidy: Int,
                             mind: Double,
                             maf: Double,
                             geno: Double,
                             overwrite: Boolean): RDD[GenotypeState] = {

    val absAdamDestination = new Path(adamDestination)
    val fs = absAdamDestination.getFileSystem(sc.hadoopConfiguration)
    // val absAssociationStr = fs.getFileStatus(relAssociationPath).getPath.toString
    val parquetInputDestination = absAdamDestination.toString.split("/").reverse.drop(1).reverse.mkString("/") + "/parquetInputFiles/"
    val parquetFiles = new Path(parquetInputDestination)

    val vcfPath = genotypesPath

    // check for ADAM formatted version of the file specified in genotypes. If it doesn't exist, convert vcf to parquet using vcf2adam.
    if (!fs.exists(parquetFiles)) {
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    } else if (overwrite) {
      fs.delete(parquetFiles, true)
      val cmdLine: Array[String] = Array[String](vcfPath, parquetInputDestination)
      Vcf2ADAM(cmdLine).run(sc)
    }

    // read in parquet files
    import sparkSession.implicits._

    val genotypes = sparkSession.read.format("parquet").load(parquetInputDestination)

    // transform the parquet-formatted genotypes into a dataFrame of GenotypeStates and convert to Dataset.
    val genotypeStates = toGenotypeStateDataFrame(genotypes, ploidy)
    // TODO: change convention so that generated variant name gets put in "names" rather than "contigName"
    val genoStatesWithNames = genotypeStates.select(
      concat($"contigName", lit("_"), $"end", lit("_"), $"alt") as "contigName",
      genotypeStates("start"),
      genotypeStates("end"),
      genotypeStates("ref"),
      genotypeStates("alt"),
      genotypeStates("sampleId"),
      genotypeStates("genotypeState"),
      genotypeStates("missingGenotypes"),
      genotypeStates("phaseSetId"))

    val gsRdd = genoStatesWithNames.as[GenotypeState].rdd

    // mind filter
    val sampleFilteredRdd = filterSamples(gsRdd, mind)

    // maf and geno filters
    val genoFilteredRdd = filterVariants(sampleFilteredRdd, geno, maf)

    val finalGenotypeStatesRdd = genoFilteredRdd.filter(_.missingGenotypes != 2)

    finalGenotypeStatesRdd
  }

  def filterSamples(genotypeStates: RDD[GenotypeState], mind: Double): RDD[GenotypeState] = {
    val mindF = sc.broadcast(mind)

    val samples = genotypeStates.map(gs => (gs.sampleId, gs.missingGenotypes, 2))
      .keyBy(_._1)
      .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3))
      .map(keyed_tup => {
        val (key, tuple) = keyed_tup
        val (sampleId, missCount, total) = tuple
        val mind = missCount.toDouble / total.toDouble
        (sampleId, mind)
      })
      .filter(_._2 <= mindF.value)
      .map(_._1)
      .collect
      .toSet
    val samples_bc = sc.broadcast(samples)
    val sampleFilteredRdd = genotypeStates.filter(gs => samples_bc.value contains gs.sampleId)

    sampleFilteredRdd
  }

  def filterVariants(genotypeStates: RDD[GenotypeState], geno: Double, maf: Double): RDD[GenotypeState] = {
    val genoF = sc.broadcast(geno)
    val mafF = sc.broadcast(maf)

    val genos = genotypeStates.map(gs => (gs.contigName, gs.missingGenotypes, gs.genotypeState, 2))
      .keyBy(_._1)
      .reduceByKey((tup1, tup2) => (tup1._1, tup1._2 + tup2._2, tup1._3 + tup2._3, tup1._4 + tup2._4))
      .map(keyed_tup => {
        val (key, tuple) = keyed_tup
        val (contigName, missCount, alleleCount, total) = tuple
        val geno = missCount.toDouble / total.toDouble
        val maf = alleleCount.toDouble / (total - missCount).toDouble
        (contigName, geno, maf, 1.0 - maf)
      })
      .filter(stats => stats._2 <= genoF.value && stats._3 >= mafF.value && stats._4 >= mafF.value)
      .map(_._1)
      .collect
      .toSet
    val genos_bc = sc.broadcast(genos)
    val genoFilteredRdd = genotypeStates.filter(gs => genos_bc.value contains gs.contigName)

    genoFilteredRdd
  }

  def loadPhenotypes(phenotypesPath: String,
                     phenoName: String,
                     oneTwo: Boolean,
                     includeCovariates: Boolean,
                     covarFile: Option[String],
                     covarNames: Option[String]): RDD[Phenotype] = {
    logInfo("Loading phenotypes from %s.".format(phenotypesPath))

    val (phenotypes, header, indexList, delimiter) = loadFileAndCheckHeader(phenotypesPath, phenoName)
    val primaryPhenoIndex = indexList(0)

    if (includeCovariates) {
      logInfo("Loading covariates from %s.".format(covarFile))
      val (covariates, covarHeader, covarIndices, delimiter) = loadFileAndCheckHeader(covarFile.get, covarNames.get)
      require(!covarNames.get.split(",").contains(phenoName), "One or more of the covariates has the same name as phenoName.")
      combineAndFilterPhenotypes(oneTwo, phenotypes, header, primaryPhenoIndex, delimiter, Option(covariates), Option(covarHeader), Option(covarIndices))
    } else {
      combineAndFilterPhenotypes(oneTwo, phenotypes, header, primaryPhenoIndex, delimiter)
    }
  }

  def loadFileAndCheckHeader(path: String, variablesString: String, isCovars: Boolean = false): (RDD[String], Array[String], Array[Int], String) = {
    val lines = sc.textFile(path).persist()
    val header = lines.first()
    val len = header.split("\t").length
    val delimiter = if (len >= 2) {
      "\t"
    } else {
      " "
    }
    val columnLabels = if (delimiter == "\t") {
      header.split("\t").zipWithIndex
    } else {
      header.split(" ").zipWithIndex
    }

    val contents = if (isCovars) {
      "Phenotypes"
    } else {
      "Covariates"
    }

    require(columnLabels.length >= 2,
      s"$contents file must have a minimum of 2 tab delimited columns. The first being some " +
        "form of sampleID, the rest being phenotype values. A header with column labels must also be present.")

    val variableNames = variablesString.split(",")
    for (variable <- variableNames) {
      val index = columnLabels.map(p => p._1).indexOf(variable)
      require(index != -1, s"$variable doesn't match any of the phenotypes specified in the header.")
    }
    (lines, columnLabels.map(p => p._1), columnLabels.map(p => p._2), delimiter)
  }

  /**
   * Loads in and filters the phenotype and covariate values from the specified files.
   *
   * @param oneTwo if True, binary phenotypes are encoded as 1 negative response, 2 positive response
   * @param phenotypes RDD of the phenotype file read from HDFS, stored as strings
   * @param covars RDD of the covars file read from HDFS, stored as strings
   * @param splitHeader phenotype file header string
   * @param splitCovarHeader covar file header string
   * @param primaryPhenoIndex index into the phenotype rows for the primary phenotype
   * @param covarIndices indices of the covariates in the covariate rows
   * @return RDD of [[Phenotype]] objects that contain the phenotypes and covariates from the specified files
   */
  private[gnocchi] def combineAndFilterPhenotypes(oneTwo: Boolean,
                                                  phenotypes: RDD[String],
                                                  splitHeader: Array[String],
                                                  primaryPhenoIndex: Int,
                                                  delimiter: String,
                                                  covars: Option[RDD[String]] = None,
                                                  splitCovarHeader: Option[Array[String]] = None,
                                                  covarIndices: Option[Array[Int]] = None): RDD[Phenotype] = {

    // TODO: NEED TO REQUIRE THAT ALL THE PHENOTYPES BE REPRESENTED BY NUMBERS.

    val fullHeader = splitHeader ++ splitCovarHeader

    val indices = if (covarIndices.isDefined) {
      val mergedIndices = covarIndices.get.map(elem => { elem + splitHeader.length })
      List(primaryPhenoIndex) ++ mergedIndices
    } else List(primaryPhenoIndex)

    val data = phenotypes.filter(line => line != splitHeader.mkString(delimiter))
      .map(line => line.split(delimiter))
      .keyBy(splitLine => splitLine(0))
      .filter(_._1 != "")

    val joinedData = if (covars.isDefined) {
      val covarData = covars.get.filter(line => line != splitCovarHeader.get.mkString(delimiter))
        .map(line => line.split(delimiter))
        .keyBy(splitLine => splitLine(0))
        .filter(_._1 != "")
      data.cogroup(covarData).map(pair => {
        val (_, (phenosIterable, covariatesIterable)) = pair
        val phenoArray = phenosIterable.toList.head
        val covarArray = covariatesIterable.toList.head
        phenoArray ++ covarArray
      })
    } else {
      data.map(p => p._2)
    }

    val finalData = joinedData
      .filter(p => p.length > 2)
      .filter(p => !indices.exists(index => isMissing(p(index))))
      .map(p => if (oneTwo) p.updated(primaryPhenoIndex, (p(primaryPhenoIndex).toDouble - 1).toString) else p)
      .map(p => Phenotype(
        indices.map(index => fullHeader(index)).mkString(","), p(0), indices.map(i => p(i).toDouble).toArray))

    phenotypes.unpersist()

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

  /**
   * Generates an RDD of observations that can be used to create or update VariantModels.
   *
   * @param genotypes RDD of GenotypeState objects
   * @param phenotypes RDD of Pheontype objects
   * @return Returns an RDD of arrays of observations (genotype + phenotype), keyed by variant
   */
  def generateObservations(genotypes: RDD[GenotypeState],
                           phenotypes: RDD[Phenotype]): RDD[(Variant, Array[(Double, Array[Double])])] = {
    // convert genotypes and phenotypes into observations
    val data = pairSamplesWithPhenotypes(genotypes, phenotypes)
    // data is RDD[((Variant, String), Iterable[(String, (GenotypeState,Phenotype))])]
    data.map(kvv => {
      val (varStr, genPhenItr) = kvv
      val (variant, phenoName) = varStr
      //.asInstanceOf[Array[(String, (GenotypeState, Phenotype[Array[Double]]))]]
      val obs = genPhenItr.map(gp => {
        val (str, (gs, pheno)) = gp
        val ob = (gs, pheno.value)
        ob
      }).toArray
      (variant, obs).asInstanceOf[(Variant, Array[(Double, Array[Double])])]
    })
  }

  /**
   *
   * @param genotypes an rdd of [[net.fnothaft.gnocchi.rdd.genotype.GenotypeState]] objects to be regressed upon
   * @param phenotypes an rdd of [[net.fnothaft.gnocchi.rdd.phenotype.Phenotype]] objects used as observations
   * @param clipOrKeepState
   * @return
   */
  def formatObservations(genotypes: RDD[GenotypeState],
                         phenotypes: RDD[Phenotype],
                         clipOrKeepState: GenotypeState => Double): RDD[((Variant, String, Int), Array[(Double, Array[Double])])] = {
    val joinedGenoPheno = genotypes.keyBy(_.sampleId).join(phenotypes.keyBy(_.sampleId))

    val keyedGenoPheno = joinedGenoPheno.map(keyGenoPheno => {
      val (_, genoPheno) = keyGenoPheno
      val (gs, pheno) = genoPheno
      val variant = Variant.newBuilder()
        .setContigName(gs.contigName)
        .setStart(gs.start)
        .setEnd(gs.end)
        .setAlternateAllele(gs.alt)
        .build()
      //        .setNames(Seq(gs.contigName).toList).build
      //      variant.setFiltersFailed(List(""))
      ((variant, pheno.phenotype, gs.phaseSetId), genoPheno)
    })
      .groupByKey()

    keyedGenoPheno.map(site => {
      val ((variant, pheno, phaseSetId), observations) = site
      val formattedObs = observations.map(p => {
        val (genotypeState, phenotype) = p
        (clipOrKeepState(genotypeState), phenotype.toDouble)
      }).toArray
      ((variant, pheno, phaseSetId), formattedObs)
    })
  }

  def extractQCPhaseSetIds(genotypeStates: RDD[GenotypeState]): RDD[(Int, String)] = {
    genotypeStates.map(g => (g.phaseSetId, g.contigName)).reduceByKey((a, b) => a)
  }

  def pairSamplesWithPhenotypes(rdd: RDD[GenotypeState],
                                phenotypes: RDD[Phenotype]): RDD[((Variant, String), Iterable[(String, (GenotypeState, Phenotype))])] = {
    rdd.keyBy(_.sampleId)
      // join together the samples with both genotype and phenotype entry
      .join(phenotypes.keyBy(_.sampleId))
      .map(kvv => {
        // unpack the entry of the joined rdd into id and actual info
        val (sampleid, gsPheno) = kvv
        // unpack the information into genotype state and pheno
        val (gs, pheno) = gsPheno

        // create contig and Variant objects and group by Variant
        // pack up the information into an Association object
        val variant = gs2variant(gs)
        ((variant, pheno.phenotype), (sampleid, gsPheno))
      }).groupByKey
  }

  def gs2variant(gs: GenotypeState): Variant = {
    val variant = new Variant()
    val contig = new Contig()
    contig.setContigName(gs.contigName)
    variant.setStart(gs.start)
    variant.setEnd(gs.end)
    variant.setAlternateAllele(gs.alt)
    variant
  }
}

//class GnocchiSqlContext private[sql] (@transient sqlContext: SQLContext) extends Serializable {
//
//}

object AuxEncoders {
  implicit def addLogEncoder: org.apache.spark.sql.Encoder[Association[AdditiveLogisticVariantModel]] = org.apache.spark.sql.Encoders.kryo[Association[AdditiveLogisticVariantModel]]
  //  implicit def domLogEncoder: org.apache.spark.sql.Encoder[Association[AdditiveLogisticVariantModel]] = org.apache.spark.sql.Encoders.kryo[Association[AdditiveLogisticVariantModel]]
  implicit def addLinEncoder: org.apache.spark.sql.Encoder[Association[AdditiveLinearVariantModel]] = org.apache.spark.sql.Encoders.kryo[Association[AdditiveLinearVariantModel]]
  //  implicit def domLinEncoder: org.apache.spark.sql.Encoder[Association[AdditiveLinearVariantModel]] = org.apache.spark.sql.Encoders.kryo[Association[AdditiveLinearVariantModel]]
  implicit def genotypeStateEncoder: org.apache.spark.sql.Encoder[GenotypeState] = org.apache.spark.sql.Encoders.kryo[GenotypeState]
}
