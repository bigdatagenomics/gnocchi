package org.bdgenomics.gnocchi.primitives.genotype

import org.bdgenomics.gnocchi.GnocchiFunSuite
import org.bdgenomics.gnocchi.GnocchiFunSuite

class GenotypeStateSuite extends GnocchiFunSuite {

  // to Double tests
  sparkTest("GenotypeState.toDouble should exit gracefully when there are no valid calls.") {
    val gs = GenotypeState("1234", "./.")
    assert(gs.toDouble == 0.0, "GenotypeState.toDouble does not deal with all missing values correctly.")
  }

  sparkTest("GenotypeState.toDouble should count the number of matches to the alternate allele.") {
    val gs = GenotypeState("1234", "1/0")
    assert(gs.toDouble == 1.0, "GenotypeState.toDouble does not correctly count the number of alternate alleles.")
  }

  sparkTest("GenotypeState.toDouble should count the number of matches to the alternate allele when there are missing values.") {
    val gs = GenotypeState("1234", "1/.")
    assert(gs.toDouble == 1.0, "GenotypeState.toDouble does not correctly count the number of alternate alleles when there are missing values.")
  }

  // to List tests
  sparkTest("GenotypeState.toList should split the genotype state into a list of genotype values: pipe delimiter.") {
    val gs = GenotypeState("1234", "1|0")
    assert(gs.toList == List[String]("1", "0"), "GenotypeState.toList does not correctly split the genotypes on pipe delimiter.")
  }

  sparkTest("GenotypeState.toList should split the genotype state into a list of genotype values: forward slash delimiter.") {
    val gs = GenotypeState("1234", "1/0")
    assert(gs.toList == List[String]("1", "0"), "GenotypeState.toList does not correctly split the genotypes on forward slash delimiter.")
  }
}
