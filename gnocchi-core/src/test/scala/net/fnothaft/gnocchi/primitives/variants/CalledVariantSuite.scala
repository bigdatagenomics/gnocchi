package net.fnothaft.gnocchi.primitives.variants

import net.fnothaft.gnocchi.GnocchiFunSuite

class CalledVariantSuite extends GnocchiFunSuite {

  // minor allele frequency tests
  ignore("CalledVariant.maf should return the frequency rate of the minor allele: minor allele is actually the major allele") {

  }

  ignore("CalledVariant.maf should return the frequency rate of the minor allele: no minor alleles present.") {

  }

  ignore("CalledVariant.maf should return the frequency rate of the minor allele: only minor alleles present.") {

  }

  ignore("CalledVariant.maf should return the frequency rate of the minor allele: all values are missing.") {

  }

  // genotyping rate tests

  ignore("CalledVariant.geno should return the fraction of missing values: all values are missing.") {

  }

  ignore("CalledVariant.geno should return the fraction of missing values: no values are missing.") {

  }

  // num valid samples tests

  ignore("CalledVariant.numValidSamples should only count samples with no missing values") {

  }

  ignore("CalledVariant.numValidSamples should return an Int") {

  }

  // num semi valid samples tests

  ignore("CalledVariant.numSemiValidSamples should count the number of samples that have some valid values.") {

  }

  ignore("CalledVariant.numSemiValidSamples should return an Int") {

  }
}
