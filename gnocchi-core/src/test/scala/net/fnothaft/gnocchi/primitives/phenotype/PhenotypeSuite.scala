package net.fnothaft.gnocchi.primitives.phenotype

import net.fnothaft.gnocchi.GnocchiFunSuite

class PhenotypeSuite extends GnocchiFunSuite {
  // toList tests
  sparkTest("Phenotype.toList should return the list of phenotypes stored inside.") {
    val pheno = Phenotype(sampleId = "1234",
      phenoName = "testPheno",
      phenotype = 12.34,
      covariates = List(12, 13, 14))

    assert(pheno.toList == List[Double](12.34, 12.0, 13.0, 14.0), "Phenotype.toList does correctly convert a phenotype object's data to a list.")
  }

  sparkTest("Phenotype constructor should be able to create a Phenotype object without covariates.") {
    val pheno = Phenotype(sampleId = "1234",
      phenoName = "testPheno",
      phenotype = 12.34,
      covariates = List())

    assert(pheno.covariates == List[Double](), "Phenotype.toList does correctly convert a phenotype object's data to a list.")
  }
}
