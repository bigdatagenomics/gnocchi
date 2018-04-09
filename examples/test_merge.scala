import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression

val genotypesPath1 = "examples/testData/time_genos_1.vcf"
val phenotypesPath1 = "examples/testData/time_phenos_1.txt"

val geno1 = sc.loadGenotypes(genotypesPath1)
val pheno1 = sc.loadPhenotypes(phenotypesPath1, "IID", "pheno_1", ",")

// val sampleFiltered1 = sc.filterSamples(geno1, mind = 0.1, ploidy = 2)
// val fullFiltered1 = sc.filterVariants(sampleFiltered1, geno = 0.1, maf = 0.1)

val broadPheno1 = sc.broadcast(pheno1)

val gm_1 = LinearGnocchiModelFactory(geno1, broadPheno1, Option(List("pheno_1")))

val genotypesPath2 = "examples/testData/time_genos_2.vcf"
val phenotypesPath2 = "examples/testData/time_phenos_2.txt"
val geno2 = sc.loadGenotypes(genotypesPath2)
val pheno2 = sc.loadPhenotypes(phenotypesPath2, "IID", "pheno_1", ",")

// val sampleFiltered2 = sc.filterSamples(geno2, mind = 0.1, ploidy = 2)
// val fullFiltered2 = sc.filterVariants(sampleFiltered2, geno = 0.1, maf = 0.1)

val broadPheno2 = sc.broadcast(pheno2)

val gm_2 = LinearGnocchiModelFactory(geno2, broadPheno2, Option(List("pheno_1")))

