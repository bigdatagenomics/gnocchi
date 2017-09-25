package net.fnothaft.gnocchi.sql

import java.io.Serializable

import net.fnothaft.gnocchi.models.variant.VariantModel
import net.fnothaft.gnocchi.models.variant.linear.AdditiveLinearVariantModel
import net.fnothaft.gnocchi.models.{GnocchiModel, GnocchiModelMetaData}
import org.bdgenomics.formats.avro.GenotypeAllele
//import net.fnothaft.gnocchi.models.linear.{ AdditiveLinearGnocchiModel, DominantLinearGnocchiModel }
//import net.fnothaft.gnocchi.models.logistic.{ AdditiveLogisticGnocchiModel, DominantLogisticGnocchiModel }
import net.fnothaft.gnocchi.models.variant.QualityControlVariantModel
//import net.fnothaft.gnocchi.models.variant.linear.{ AdditiveLinearVariantModel, DominantLinearVariantModel }
//import net.fnothaft.gnocchi.models.variant.logistic.{ AdditiveLogisticVariantModel, DominantLogisticVariantModel }
import net.fnothaft.gnocchi.primitives.genotype.GenotypeState
import net.fnothaft.gnocchi.primitives.phenotype.Phenotype
import net.fnothaft.gnocchi.primitives.variants.CalledVariant
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, DataFrame, Dataset, SparkSession }
import org.apache.spark.sql.functions.{ array, col, concat, lit, sum, typedLit, udf, when }
import org.apache.spark.sql.types.{ ArrayType, DoubleType }
import org.bdgenomics.adam.cli.Vcf2ADAM
import org.bdgenomics.formats.avro.{ Contig, Variant }
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.adam.rdd.ADAMContext._
import htsjdk.samtools.ValidationStringency

import scala.io.Source.fromFile
import scala.collection.JavaConversions._

object GnocchiSession {

  // Add GnocchiContext methods
  implicit def sparkContextToGnocchiContext(sc: SparkContext): GnocchiSession =
    new GnocchiSession(sc)

}

class GnocchiSession(@transient val sc: SparkContext) extends Serializable with Logging {

  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  //  def convertVCF(vcfPath: String,
  //                 destination: String,
  //                 overwrite: Boolean): Path = {
  //
  //    val adamDestination = new Path(destination)
  //    val parquetFiles = new Path(vcfPath)
  //    val fs = adamDestination.getFileSystem(sc.hadoopConfiguration)
  //
  //    if (!fs.exists(parquetFiles)) {
  //      val cmdLine: Array[String] = Array[String](vcfPath, destination)
  //      Vcf2ADAM(cmdLine).run(sc)
  //    } else if (overwrite) {
  //      fs.delete(parquetFiles, true)
  //      val cmdLine: Array[String] = Array[String](vcfPath, destination)
  //      Vcf2ADAM(cmdLine).run(sc)
  //    }
  //
  //    adamDestination
  //  }

  //    def loadGenotypesAsTextWithADAM(genotypesPath: String,
  //                                    ploidy: Int,
  //                                    mind: Option[Double],
  //                                    maf: Option[Double],
  //                                    geno: Option[Double]): DataFrame = {
  //
  //      val validationStringency = ValidationStringency.valueOf("STRICT")
  //      val variantContextRDD = sc.loadVcf(genotypesPath, validationStringency)
  //

  // import sparkSession.implicits._
  //
  //      val genotypes = sparkSession.read.format("parquet").load(genotypesPath)
  //
  //      val genotypeDF = toGenotypeStateDataFrame(genotypes, ploidy)
  //
  //      val genoStatesWithNames = genotypeDF.select(
  //        $"contigName" as "chromosome",
  //        $"start" as "position",
  //        genotypeDF("end"),
  //        genotypeDF("ref"),
  //        genotypeDF("alt"),
  //        genotypeDF("sampleId"),
  //        genotypeDF("genotypeState"),
  //        genotypeDF("missingGenotypes"),
  //        genotypeDF("phaseSetId"))

  //    val sampleFilteredDF = filterSamples(genoStatesWithNames, mind)
  //
  //    val genoFilteredDF = filterVariants(sampleFilteredDF, geno, maf)

  //    genoFilteredDF

  //    val finalGenotypeStatesRdd = genoFilteredDF.filter($"missingGenotypes" != 2)

  //    finalGenotypeStatesRdd
  //    }

  //  private def toGenotypeStateDataFrame(gtFrame: DataFrame, ploidy: Int): DataFrame = {
  //    // generate expression
  //    val genotypeState = (0 until ploidy).map(i => {
  //      val c: Column = when(gtFrame("alleles").getItem(i) === "REF", 1).otherwise(0)
  //      c
  //    }).reduce(_ + _)
  //
  //    val missingGenotypes = (0 until ploidy).map(i => {
  //      val c: Column = when(gtFrame("alleles").getItem(i) === "NO_CALL", 1).otherwise(0)
  //      c
  //    }).reduce(_ + _)
  //
  //    // is this correct? or should we change the column to nullable?
  //    val phaseSetId: Column = when(gtFrame("phaseSetId").isNull, 0).otherwise(gtFrame("phaseSetId"))
  //
  //    gtFrame.select(gtFrame("variant.contigName").as("contigName"),
  //      gtFrame("variant.start").as("start"),
  //      gtFrame("variant.end").as("end"),
  //      gtFrame("variant.referenceAllele").as("ref"),
  //      gtFrame("variant.alternateAllele").as("alt"),
  //      gtFrame("sampleId"),
  //      genotypeState.as("genotypeState"),
  //      missingGenotypes.as("missingGenotypes"),
  //      phaseSetId.as("phaseSetId"))
  //  }

  def filterSamples(genotypes: Dataset[CalledVariant], mind: Double, ploidy: Double): Dataset[CalledVariant] = {
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
    def filtersDF = genotypes.map(x => (x.uniqueID, x.maf, x.geno)).toDF("uniqueID", "maf", "geno")
    def mafFiltered = genotypes.join(filtersDF, "uniqueID")
      .filter($"maf" >= maf && lit(1) - $"maf" >= maf && $"geno" <= geno)
      .drop("maf", "geno")
      .as[CalledVariant]

    mafFiltered
  }

  def loadGenotypesAsText(genotypesPath: String, fromAdam: Boolean = false): Dataset[CalledVariant] = {
    if (fromAdam) {
      val vcRdd = sc.loadVcf(genotypesPath)
      vcRdd.rdd.map(vc => {
        val variant = vc.variant.variant
        CalledVariant(variant.getContigName.toInt,
          variant.getStart.intValue(),
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
    } else {
      // ToDo: Deal with multiple Alts
      val stringVariantDS = sparkSession.read.textFile(genotypesPath).filter(row => !row.startsWith("##"))

      val variantDF = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", "\t")
        .csv(stringVariantDS)

      // drop the variant level metadata
      val samples = variantDF.schema.fields.drop(9).map(x => x.name)

      val groupedSamples = variantDF
        .select($"ID", array(samples.head, samples.tail: _*))
        .as[(String, Array[String])]
      val typedGroupedSamples = groupedSamples
        .map(row => (row._1, samples.zip(row._2).map(x => GenotypeState(x._1, x._2))))
        .toDF("ID", "samples")
        .as[(String, Array[GenotypeState])]

      val formattedRawDS = variantDF.drop(samples: _*).join(typedGroupedSamples, "ID")

      val formattedVariantDS = formattedRawDS.toDF(
        "uniqueID",
        "chromosome",
        "position",
        "referenceAllele",
        "alternateAllele",
        "qualityScore",
        "filter",
        "info",
        "format",
        "samples")

      formattedVariantDS.as[CalledVariant]
    }
  }

  /**
   *
   * @note assume all covarNames are in covariate file
   * @note assume phenoName is in phenotype file
   * @note assume that
   *
   *
   * @param phenotypesPath
   * @param phenoName
   * @param oneTwo
   * @param delimiter
   * @param covarPath
   * @param covarNames
   * @return
   */
  def loadPhenotypes(phenotypesPath: String,
                     primaryID: String,
                     phenoName: String,
                     delimiter: String,
                     covarPath: Option[String] = None,
                     covarNames: Option[List[String]] = None): Map[String, Phenotype] = {

    logInfo("Loading phenotypes from %s.".format(phenotypesPath))

    // ToDo: keeps these operations on one machine, because phenotypes are small.
    val prelimPhenotypesDF = sparkSession.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", delimiter)
      .load(phenotypesPath)

    val phenoHeader = prelimPhenotypesDF.schema.fields.map(_.name)

    require(phenoHeader.contains(phenoName),
      s"The primary phenotype, '$phenoName' does not exist in the specified file, '$phenotypesPath'")
    require(phenoHeader.contains(primaryID),
      s"The primary sample ID, '$primaryID' does not exist in the specified file, '$phenotypesPath'")

    val phenotypesDF = prelimPhenotypesDF
      .select(primaryID, phenoName)
      .toDF("sampleId", "phenotype")

    val covariateDF = if (covarPath.isDefined) {
      val prelimCovarDF = sparkSession.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .load(covarPath.get)

      val covarHeader = prelimCovarDF.schema.fields.map(_.name)

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
      None
    }

    val phenoCovarDF = if (covariateDF.isDefined) {
      val joinedDF = phenotypesDF.join(covariateDF.get, Seq("sampleId"))
      joinedDF.withColumn("covariates", array(covarNames.get.head, covarNames.get.tail: _*))
        .select("sampleId", "phenotype", "covariates")
    } else {
      phenotypesDF.withColumn("covariates", lit(null).cast(ArrayType(DoubleType)))
    }

    phenoCovarDF.withColumn("phenoName", lit(phenoName)).as[Phenotype].collect().map(x => (x.sampleId, x)).toMap
  }
}