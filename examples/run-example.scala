import org.bdgenomics.gnocchi.sql.GnocchiSession._
import org.bdgenomics.gnocchi.algorithms.siteregression.LinearSiteRegression
val genotypesPath = "examples/testData/time_genos_1.vcf"
val phenotypesPath = "examples/testData/tab_time_phenos_1.txt"
val geno = sc.loadGenotypes(genotypesPath)
val pheno = sc.loadPhenotypes(phenotypesPath, "IID", "pheno_1", "\t", Option(phenotypesPath), Option(List("pheno_4", "pheno_5")))

val filteredGeno = sc.filterSamples(geno, mind = 0.1, ploidy = 2)
val filteredGenoVariants = sc.filterVariants(filteredGeno, geno = 0.1, maf = 0.1)

val broadPheno = sc.broadcast(pheno)

val assoications = LinearRegression(geno, broadPheno)
