package net.fnothaft.gnocchi.primitives.variants

import net.fnothaft.gnocchi.primitives.genotype.GenotypeState

case class CalledVariant(chromosome: Int,
                         position: Int,
                         uniqueID: String,
                         referenceAllele: String,
                         alternateAllele: String,
                         qualityScore: String,
                         filter: String,
                         info: String,
                         format: String,
                         samples: List[GenotypeState]) extends Product {

  def maf: Double = {
    val ploidy = samples.head.value.split("/|\\|").length
    val sampleValues: List[String] = samples.flatMap(_.value.split("/|\\|"))
    val missingCount = sampleValues.count(_ == ".")
    val alleleCount = sampleValues.filter(_ != ".").map(_.toDouble).sum

    alleleCount / (ploidy * sampleValues.length - missingCount)
  }

  def geno: Double = {
    val ploidy = samples.head.value.split("/|\\|").length
    val sampleValues: List[String] = samples.flatMap(_.value.split("/|\\|"))
    val missingCount = sampleValues.count(_ == ".")

    missingCount.toDouble / (ploidy * sampleValues.length).toDouble
  }
}