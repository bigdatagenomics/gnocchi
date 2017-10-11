import org.bdgenomics.gnocchi.sql.GnocchiSession._
val genotypesPath = "testData/time_phenos.vcf"
val phenotypesPath = "testData/tab_time_phenos.txt"
val geno = sc.loadGenotypesAsText(genotypesPath)
val pheno = sc.loadPhenotypes(phenotypesPath, "IID", "pheno_1", "\t", Option(phenotypesPath), Option(List("pheno_4", "pheno_5")))

val filteredGeno = sc.filterSamples(geno, mind = 0.1, ploidy = 2)
val filteredGenoVariants = sc.filterVariants(filteredGeno, geno = 0.1, maf = 0.1)

val broadPheno = sc.broadcast(pheno)
