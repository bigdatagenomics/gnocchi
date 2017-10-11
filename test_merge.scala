import org.bdgenomics.gnocchi.sql.GnocchiSession._
val genotypesPath1 = "testData/time_genos_1.vcf"
val phenotypesPath1 = "testData/tab_time_phenos_1.txt"
val geno1 = sc.loadGenotypes(genotypesPath1)
val pheno1 = sc.loadPhenotypes(phenotypesPath1, "IID", "pheno_1", "\t")

val sampleFiltered1 = sc.filterSamples(geno1, mind = 0.1, ploidy = 2)
val fullFiltered1 = sc.filterVariants(sampleFiltered1, geno = 0.1, maf = 0.1)

val broadPheno1 = sc.broadcast(pheno1)

// val genotypesPath2 = "testData/time_genos_2.vcf"
// val phenotypesPath2 = "testData/tab_time_phenos_2.txt"
// val geno2 = sc.loadGenotypesAsText(genotypesPath2)
// val pheno2 = sc.loadPhenotypes(phenotypesPath2, "IID", "pheno_1", "\t", Option(phenotypesPath2), Option(List("pheno_2", "pheno_3")))

// val sampleFiltered2 = sc.filterSamples(geno2, mind = 0.1, ploidy = 2)
// val fullFiltered2 = sc.filterVariants(sampleFiltered2, geno = 0.1, maf = 0.1)

// val broadPheno2 = sc.broadcast(pheno2)

import org.bdgenomics.gnocchi.algorithms.siteregression.AdditiveLinearRegression

val assoc_1 = AdditiveLinearRegression(fullFiltered1, broadPheno1)
// val assoc_2 = AdditiveLinearRegression(fullFiltered2, broadPheno2)
