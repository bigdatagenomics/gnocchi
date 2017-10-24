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
package org.bdgenomics.gnocchi.sql

import java.io.Serializable

import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{ array, lit, udf, when }
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.Logging
import java.nio.file.{ Files, Paths }

import org.apache.hadoop.fs.Path
import org.bdgenomics.gnocchi.models.variant.VariantModel

import scala.collection.JavaConversions._
import scala.io.StdIn.readLine

object GnocchiSession {
  implicit def sparkContextToGnocchiSession(sc: SparkContext): GnocchiSession = new GnocchiSession(sc)
}

/**
 * The GnocchiSession provides functions on top of a SparkContext for loading and
 * analyzing genome data.
 *
 * @param sc The SparkContext to wrap.
 */
class GnocchiSession(@transient val sc: SparkContext) extends Serializable with Logging {

  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  /**
   * Returns a filtered Dataset of CalledVariant objects, where all values with
   * fewer samples than the mind threshold are filtered out.
   *
   * @param genotypes The Dataset of CalledVariant objects to filter on
   * @param mind The percentage threshold of samples to have filled in; values
   *             with fewer samples will be removed in this operation.
   * @param ploidy The number of sets of chromosomes
   *
   * @return Returns an updated Dataset with values removed, as specified by the
   *         filtering
   */
  def filterSamples(genotypes: Dataset[CalledVariant], mind: Double, ploidy: Double): Dataset[CalledVariant] = {
    require(mind >= 0.0 && mind <= 1.0, "`mind` value must be between 0.0 to 1.0 inclusive.")
    val sampleIds = genotypes.first.samples.map(x => x.sampleID)
    val separated = genotypes.select($"uniqueID" +: sampleIds.indices.map(idx => $"samples"(idx) as sampleIds(idx)): _*)

    val filtered = separated.select($"uniqueID" +: sampleIds.map(sampleId =>
      when(separated(sampleId).getField("value") === "./.", 2)
        .when(separated(sampleId).getField("value").endsWith("."), 1)
        .when(separated(sampleId).getField("value").startsWith("."), 1)
        .otherwise(0) as sampleId): _*)

    val summed = filtered.drop("uniqueID").groupBy().sum().toDF(sampleIds: _*).select(array(sampleIds.head, sampleIds.tail: _*)).as[Array[Double]].collect.toList.head
    val count = filtered.count()
    val missingness = summed.map(_ / (ploidy * count))
    val samplesWithMissingness = sampleIds.zip(missingness)

    val keepers = samplesWithMissingness.filter(x => x._2 <= mind).map(x => x._1)

    val filteredDF = separated.select($"uniqueID", array(keepers.head, keepers.tail: _*)).toDF("uniqueID", "samples").as[(String, Array[String])]

    genotypes.drop("samples").join(filteredDF, "uniqueID").as[CalledVariant]
  }

  /**
   * Returns a filtered Dataset of CalledVariant objects, where all variants with
   * values less than the specified geno or maf threshold are filtered out.
   *
   * @param genotypes The Dataset of CalledVariant objects to filter on
   * @param geno The percentage threshold for geno values for each CalledVariant
   *             object
   * @param maf The percentage threshold for Minor Allele Frequency for each
   *            CalledVariant object
   *
   * @return Returns an updated Dataset with values removed, as specified by the
   *         filtering
   */
  def filterVariants(genotypes: Dataset[CalledVariant], geno: Double, maf: Double): Dataset[CalledVariant] = {
    require(maf >= 0.0 && maf <= 1.0, "`maf` value must be between 0.0 to 1.0 inclusive.")
    require(geno >= 0.0 && geno <= 1.0, "`geno` value must be between 0.0 to 1.0 inclusive.")
    val filtersDF = genotypes.map(x => (x.uniqueID, x.maf, x.geno)).toDF("uniqueID", "maf", "geno")
    val mafFiltered = genotypes.join(filtersDF, "uniqueID")
      .filter($"maf" >= maf && lit(1) - $"maf" >= maf && $"geno" <= geno)
      .drop("maf", "geno")
      .as[CalledVariant]

    mafFiltered
  }

  /**
   * Returns a modified Dataset of CalledVariant objects, where any value with a
   * maf > 0.5 is recoded. The recoding is specified as flipping the referenceAllele
   * and alternateAllele when the frequency of alt is greater than that of ref.
   *
   * @param genotypes The Dataset of CalledVariant objects to recode
   *
   * @return Returns an updated Dataset that has been recoded
   */
  def recodeMajorAllele(genotypes: Dataset[CalledVariant]): Dataset[CalledVariant] = {
    val minorAlleleF = genotypes.map(x => (x.uniqueID, x.maf)).toDF("uniqueID", "maf")
    val genoWithMaf = genotypes.join(minorAlleleF, "uniqueID")
    val toRecode = genoWithMaf.filter($"maf" > 0.5).drop("maf").as[CalledVariant]
    val recoded = toRecode.map(
      x => {
        CalledVariant(x.chromosome,
          x.position,
          x.uniqueID,
          x.alternateAllele,
          x.referenceAllele,
          x.qualityScore,
          x.filter,
          x.info,
          x.format,
          x.samples.map(geno => GenotypeState(geno.sampleID, geno.toList.map {
            case "0" => "1"
            case "1" => "0"
            case "." => "."
          }.mkString("/"))))
      }).as[CalledVariant]
    // Note: the below .map(a => a) is a hack solution to the fact that spark cannot union two
    // datasets of the same type that have reordered columns. See issue: https://issues.apache.org/jira/browse/SPARK-21109
    genoWithMaf.filter($"maf" <= 0.5).drop("maf").as[CalledVariant].map(a => a).union(recoded)
  }

  /**
   * @note currently this does not enforce that the uniqueID is, in fact, unique across the dataset. Checking uniqueness
   *       would require a shuffle, which adds overhead that might not be necessary right now.
   *
   * @param genotypesPath A string specifying the location in the file system of the genotypes file to load in.
   * @return a [[Dataset]] of [[CalledVariant]] objects loaded from a vcf file
   */
  def loadGenotypes(genotypesPath: String): Dataset[CalledVariant] = {

    val genoFile = new Path(genotypesPath)
    val fs = genoFile.getFileSystem(sc.hadoopConfiguration)
    require(fs.exists(genoFile), s"Specified genotypes file path does not exist: ${genotypesPath}")

    val vcRdd = sc.loadVcf(genotypesPath)
    vcRdd.rdd.map(vc => {
      val variant = vc.variant.variant
      CalledVariant(variant.getContigName.toInt,
        variant.getEnd.intValue(),
        variant.getNames.get(0),
        variant.getReferenceAllele,
        variant.getAlternateAllele,
        "", // quality score
        "", // filter
        "", // info
        "", // format
        vc.genotypes.map(geno => GenotypeState(geno.getSampleId, geno.getAlleles.map {
          case GenotypeAllele.REF                            => "0"
          case GenotypeAllele.ALT | GenotypeAllele.OTHER_ALT => "1"
          case GenotypeAllele.NO_CALL | _                    => "."
        }.mkString("/"))).toList)
    }).toDS
  }

  /**
   * Returns a map of phenotype name to phenotype object, which is loaded from
   * a file, specified by phenotypesPath
   *
   * @param phenotypesPath A string specifying the location in the file system
   *                       of the phenotypes file to load in.
   * @param primaryID The primary sample ID
   * @param phenoName The primary phenotype
   * @param delimiter The delimiter used in the input file
   * @param covarPath Optional parameter specifying the location in the file
   *                  system of the covariants file
   * @param covarNames Optional paramter specifying the names of the covariants
   *                   detailed in the covariants file
   * @param covarDelimiter The delimiter used in the covariants file
   *
   * @return A Map of phenotype name to Phenotype object
   */
  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None,
                     covarDelimiter: String = "\t",
                     missing: List[Int] = List(-9)): Map[String, Phenotype] = {

    val phenoFile = new Path(phenotypesPath)
    val fs = phenoFile.getFileSystem(sc.hadoopConfiguration)
    require(fs.exists(phenoFile), s"Specified genotypes file path does not exits: ${phenotypesPath}")
    logInfo("Loading phenotypes from %s.".format(phenotypesPath))

    // ToDo: keeps these operations on one machine, because phenotypes are small.
    val prelimPhenotypesDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(phenotypesPath)

    val phenoHeader = prelimPhenotypesDF.schema.fields.map(_.name)

    require(phenoHeader.length > 1,
      s"The specified delimiter '$delimiter' does not separate fields in the specified file, '$phenotypesPath'")
    require(phenoHeader.contains(phenoName),
      s"The primary phenotype, '$phenoName' does not exist in the specified file, '$phenotypesPath'")
    require(phenoHeader.contains(primaryID),
      s"The primary sample ID, '$primaryID' does not exist in the specified file, '$phenotypesPath'")

    val phenotypesDF = prelimPhenotypesDF
      .select(primaryID, phenoName)
      .toDF("sampleId", "phenotype")

    val covariateDF = if (covarPath.isDefined && covarNames.isDefined) {
      val prelimCovarDF = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", covarDelimiter)
        .load(covarPath.get)

      val covarHeader = prelimCovarDF.schema.fields.map(_.name)

      require(covarHeader.length > 1,
        s"The specified delimiter '$covarDelimiter' does not separate fields in the specified file, '${covarPath.get}'")
      require(covarNames.get.forall(covarHeader.contains(_)),
        s"One of the covariates, '%s' does not exist in the specified file, '%s'".format(covarNames.get.toString(), covarPath.get))
      require(covarHeader.contains(primaryID),
        s"The primary sample ID, '$primaryID' does not exist in the specified file, '%s'".format(covarPath.get))
      require(!covarNames.get.contains(phenoName),
        s"The primary phenotype, '$phenoName' cannot be listed as a covariate. '%s'".format(covarNames.get.toString()))

      Option(prelimCovarDF
        .select(primaryID, covarNames.get: _*)
        .toDF("sampleId" :: covarNames.get: _*))
    } else {
      require(covarPath.isEmpty && covarNames.isEmpty, "Covariate path needs to be specified with covariate names.")
      None
    }

    val phenoCovarDF = if (covariateDF.isDefined) {
      val joinedDF = phenotypesDF.join(covariateDF.get, Seq("sampleId"))
      joinedDF.withColumn("covariates", array(covarNames.get.head, covarNames.get.tail: _*))
        .select("sampleId", "phenotype", "covariates")
    } else {
      phenotypesDF.withColumn("covariates", array())
    }

    phenoCovarDF
      .withColumn("phenoName", lit(phenoName))
      .as[Phenotype]
      .collect()
      .filter(x => !missing.contains(x.phenotype) && x.covariates.forall(!missing.contains(_)))
      .map(x => (x.sampleId, x)).toMap
  }

  def saveAssociations[A <: VariantModel[A]](associations: Dataset[A],
                                             outPath: String,
                                             saveAsText: Boolean = false,
                                             forceSave: Boolean) = {
    // save dataset
    val associationsFile = new Path(outPath)
    val fs = associationsFile.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(associationsFile)) {
      if (forceSave) {
        fs.delete(associationsFile)
      } else {
        val input = readLine(s"Specified output file ${outPath} already exists. Overwrite? (y/n)> ")
        if (input.equalsIgnoreCase("y") || input.equalsIgnoreCase("yes")) {
          fs.delete(associationsFile)
        }
      }
    }

    val assoc = associations.map(x => (x.uniqueID, x.chromosome, x.position, x.association.pValue, x.association.weights(1), x.association.numSamples))
      .withColumnRenamed("_1", "uniqueID")
      .withColumnRenamed("_2", "chromosome")
      .withColumnRenamed("_3", "position")
      .withColumnRenamed("_4", "pValue")
      .withColumnRenamed("_5", "beta")
      .withColumnRenamed("_6", "numSamples")
      .sort($"pValue".asc).coalesce(5)

    // enables saving as parquet or human readable text files
    if (saveAsText) {
      assoc.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").save(outPath)
    } else {
      assoc.toDF.write.parquet(outPath)
    }
  }
}
