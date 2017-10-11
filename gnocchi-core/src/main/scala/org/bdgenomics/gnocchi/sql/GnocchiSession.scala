package org.bdgenomics.gnocchi.sql

import java.io.Serializable

import org.bdgenomics.formats.avro.GenotypeAllele
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{ array, lit, udf }
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.utils.misc.Logging

import java.nio.file.{ Paths, Files }
import scala.collection.JavaConversions._

object GnocchiSession {
  // Add GnocchiContext methods
  implicit def sparkContextToGnocchiSession(sc: SparkContext): GnocchiSession =
    new GnocchiSession(sc)
}

class GnocchiSession(@transient val sc: SparkContext) extends Serializable with Logging {

  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  def filterSamples(genotypes: Dataset[CalledVariant], mind: Double, ploidy: Double): Dataset[CalledVariant] = {
    require(mind >= 0.0 && mind <= 1.0, "`mind` value must be between 0.0 to 1.0 inclusive.")
    val sampleIds = genotypes.first.samples.map(x => x.sampleID)
    val separated = genotypes.select($"uniqueID" +: sampleIds.indices.map(idx => $"samples"(idx) as sampleIds(idx)): _*)

    val missingFn: String => Int = _.split("/|\\|").count(_ == ".")
    val missingUDF = udf(missingFn)

    val filtered = separated.select($"uniqueID" +: sampleIds.map(sampleId => missingUDF(separated(sampleId).getField("value")) as sampleId): _*)

    val summed = filtered.drop("uniqueID").groupBy().sum().toDF(sampleIds: _*)
    val count = filtered.count()

    val missingness = summed.select(sampleIds.map(sampleId => summed(sampleId) / (ploidy * count) as sampleId): _*)
    val plainMissingness = missingness.select(array(sampleIds.head, sampleIds.tail: _*)).as[Array[Double]].head

    val samplesWithMissingness = sampleIds.zip(plainMissingness)
    val keepers = samplesWithMissingness.filter(x => x._2 <= mind).map(x => x._1)

    val filteredDF = separated.select($"uniqueID", array(keepers.head, keepers.tail: _*)).toDF("uniqueID", "samples").as[(String, Array[String])]

    genotypes.drop("samples").join(filteredDF, "uniqueID").as[CalledVariant]
  }

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
    require(Files.exists(Paths.get(genotypesPath)), s"Specified genotypes file path does not exist: ${genotypesPath}")
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

  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None,
                     covarDelimiter: String = "\t"): Map[String, Phenotype] = {

    require(Files.exists(Paths.get(phenotypesPath)), s"Specified genotypes file path does not exits: ${phenotypesPath}")
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
        s"The specified delimiter '$delimiter' does not separate fields in the specified file, '$phenotypesPath'")
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

    phenoCovarDF.withColumn("phenoName", lit(phenoName)).as[Phenotype].collect().map(x => (x.sampleId, x)).toMap
  }
}