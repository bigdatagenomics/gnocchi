package org.bdgenomics.gnocchi.algorithms.siteregression

import breeze.linalg.{ DenseMatrix, DenseVector }
import org.bdgenomics.gnocchi.models.variant.{ LinearVariantModel, VariantModel }
import org.bdgenomics.gnocchi.primitives.association.{ Association, LinearAssociation }
import org.bdgenomics.gnocchi.primitives.genotype.GenotypeState
import org.bdgenomics.gnocchi.primitives.phenotype.Phenotype
import org.bdgenomics.gnocchi.primitives.variants.CalledVariant
import org.bdgenomics.gnocchi.utils.GnocchiFunSuite

object TestSiteRegression extends SiteRegression[LinearVariantModel, LinearAssociation]

class SiteRegressionSuite extends GnocchiFunSuite {
  // Ignore because there is no reliable way to get order from the map consistently
  sparkTest("SiteRegression.prepareDesignMatrix should place the genotype value in the second column of the design matrix.") {
    val phenotypes = Map("7677" -> Phenotype("7677", "pheno_1", 1, List(1, 1, 1)))
    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T", Map("7677" -> GenotypeState(0, 2, 0)))

    val (data, label) = TestSiteRegression.prepareDesignMatrix(cv, phenotypes, "ADDITIVE")

    val genos = DenseVector(2.0)
    assert(data(::, 1) == genos, "SiteRegression.prepareDesignMatrix places genos in the wrong place")
  }

  sparkTest("SiteRegression.prepareDesignMatrix should place the covariates in columns 2 through n in the design matrix") {
    val phenotypes = Map("7677" -> Phenotype("7677", "pheno_1", 9, List(1, 2, 3)))
    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T", Map("7677" -> GenotypeState(0, 2, 0)))

    val (data, label) = TestSiteRegression.prepareDesignMatrix(cv, phenotypes, "ADDITIVE")

    val genos = DenseVector(1.0, 2.0, 3.0)

    val covs = data(::, 2 to -1)

    assert(covs.toArray === genos.toArray, "Covariates are wrong.")
  }

  sparkTest("SiteRegression.prepareDesignMatrix should produce a `(DenseMatrix[Double], DenseVector[Double])`") {
    val gs = createSampleGenotypeStates(num = 1, maf = 0.25, geno = 0.1, ploidy = 2)
    val cv = createSampleCalledVariant(samples = Option(gs))
    val phenos = createSamplePhenotype(calledVariant = Option(cv), numCovariate = 3)

    val XandY = TestSiteRegression.prepareDesignMatrix(cv, phenos, "ADDITIVE")

    assert(XandY.isInstanceOf[(DenseMatrix[Double], DenseVector[Double])], "SiteRegression.prepareDesignMatrix returned an incorrect type.")
  }

  // This test is bulky, rewrite
  sparkTest("SiteRegression.prepareDesignMatrix should produce a matrix with missing values filtered out: ADDITIVE.") {
    val phenotypes = Map(
      "7677" -> Phenotype("7677", "pheno_1", 1, List(1, 1, 1)),
      "5218" -> Phenotype("5218", "pheno_1", 2, List(2, 2, 2)),
      "1939" -> Phenotype("1939", "pheno_1", 3, List(3, 3, 3)))

    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        // have one genotype that doesn't have a phenotype
        "9999" -> GenotypeState(1, 1, 0),
        // have one genotype that has missing calls
        "5218" -> GenotypeState(0, 1, 1),
        // have one genotype that is valid
        "1939" -> GenotypeState(0, 2, 0)))

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenotypes, "ADDITIVE")
    // Verify length of X and Y matrices
    assert(x.rows === 1)
    assert(y.length === 1)

    // Verify contents of matrices, function should filter out both genotypes and phenotypes
    assert(x(::, 1).toArray === Array(cv.samples("1939").additive))
    assert(y.toArray === Array(phenotypes("1939").phenotype))
  }

  sparkTest("SiteRegression.prepareDesignMatrix should produce a matrix with missing values filtered out: DOMINANT.") {
    val phenotypes = Map(
      "7677" -> Phenotype("7677", "pheno_1", 1, List(1, 1, 1)),
      "5218" -> Phenotype("5218", "pheno_1", 2, List(2, 2, 2)),
      "1939" -> Phenotype("1939", "pheno_1", 3, List(3, 3, 3)))

    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T",
      Map(
        // have one genotype that doesn't have a phenotype
        "9999" -> GenotypeState(1, 1, 0),
        // have one genotype that has missing calls
        "5218" -> GenotypeState(0, 1, 1),
        // have one genotype that is valid
        "1939" -> GenotypeState(0, 2, 0)))

    val (x, y) = LinearSiteRegression.prepareDesignMatrix(cv, phenotypes, "DOMINANT")
    // Verify length of X and Y matrices
    assert(x.rows === 1)
    assert(y.length === 1)

    // Verify contents of matrices, function should filter out both genotypes and phenotypes
    assert(x(::, 1).toArray === Array(cv.samples("1939").dominant))
    assert(y.toArray === Array(phenotypes("1939").phenotype))
  }

  sparkTest("SiteRegression.prepareDesignMatrix should create a label vector filled with phenotype values.") {
    val phenotypes = Map("7677" -> Phenotype("7677", "pheno_1", 9, List(1, 1, 1)))
    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T", Map("7677" -> GenotypeState(0, 2, 0)))

    val (data, label) = TestSiteRegression.prepareDesignMatrix(cv, phenotypes, "ADDITIVE")

    val genos = DenseVector(9.0)

    assert(label.toArray === genos.toArray, "SiteRegression.prepareDesignMatrix places label in the wrong place")
  }

  sparkTest("LinearSiteRegression.prepareDesignMatrix should correctly take into account the allelic assumption: ADDITIVE") {
    val phenotypes = Map("7677" -> Phenotype("7677", "pheno_1", 9, List(1, 1, 1)))
    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T", Map("7677" -> GenotypeState(0, 2, 0)))

    val (data, label) = TestSiteRegression.prepareDesignMatrix(cv, phenotypes, "ADDITIVE")

    assert(data(::, 1).toArray === Array(cv.samples("7677").additive), "SiteRegression.prepareDesignMatrix incorrectly takes into account allelic assumption")
  }

  sparkTest("LinearSiteRegression.prepareDesignMatrix should correctly take into account the allelic assumption: DOMINANT") {
    val phenotypes = Map("7677" -> Phenotype("7677", "pheno_1", 9, List(1, 1, 1)))
    val cv = CalledVariant("rs8330247", 14, 21373362, "C", "T", Map("7677" -> GenotypeState(0, 2, 0)))

    val (data, label) = TestSiteRegression.prepareDesignMatrix(cv, phenotypes, "DOMINANT")

    assert(data(::, 1).toArray === Array(cv.samples("7677").dominant), "SiteRegression.prepareDesignMatrix incorrectly takes into account allelic assumption")
  }
}
