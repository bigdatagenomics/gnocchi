package org.bdgenomics.gnocchi.primitives.variants

import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState

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

  /**
   * @return the minor allele frequency across all samples for this variant
   */
  def maf: Double = {
    val ploidy = samples.head.toList.length
    val sampleValues: List[String] = samples.flatMap(_.toList)
    val missingCount = sampleValues.count(_ == ".")
    val alleleCount = sampleValues.filter(_ != ".").map(_.toDouble).sum

    assert(sampleValues.length > missingCount, "Variant has entirely missing row. Fix by filtering variants with geno = 1.0")

    alleleCount / (sampleValues.length - missingCount)
  }

  /**
   * @return The fraction of missing values for this variant values across all samples
   */
  def geno: Double = {
    val ploidy = samples.head.toList.length
    val sampleValues: List[String] = samples.flatMap(_.toList)
    val missingCount = sampleValues.count(_ == ".")

    missingCount.toDouble / sampleValues.length.toDouble
  }

  /**
   * @return Number of samples that have all valid values (none missing)
   */
  def numValidSamples: Int = {
    samples.count(x => !x.value.contains("."))
  }

  /**
   * @return Number of samples that have some valid values (could be some missing)
   */
  def numSemiValidSamples: Int = {
    samples.count(x => x.toList.count(_ != ".") > 0)
  }
}