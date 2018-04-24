from bdgenomics.gnocchi.models.linearGnocchiModel import LinearGnocchiModel

from bdgenomics.gnocchi.core.gnocchiSession import GnocchiSession
from bdgenomics.gnocchi.models.logisticGnocchiModel import LogisticGnocchiModel
from bdgenomics.gnocchi.core.regressPhenotypes import RegressPhenotypes

genotypesPath1 = "examples/testData/1snp10samples.vcf"
phenotypesPath1 = "examples/testData/10samples1Phenotype.txt"

gs = GnocchiSession(spark)
genos = gs.loadGenotypes(genotypesPath1)
phenos = gs.loadPhenotypes(phenotypesPath1, "SampleID", "pheno1", "\t")

lgm = LinearGnocchiModel.New(spark, genos, phenos, ["AD"], ["GI"])
lgm2 = LinearGnocchiModel.New(spark, genos, phenos, ["AD"], ["GI"])


lgm.mergeGnocchiModel(lgm2)

logm = LogisticGnocchiModel.New(spark, genos, phenos, ["AD"], ["GI"])
logm2 = LogisticGnocchiModel.New(spark, genos, phenos, ["AD"], ["GI"])


logm.mergeGnocchiModel(lgm2)

rp = RegressPhenotypes(spark)
rp.apply("examples/testData/1snp10samples.vcf examples/testData/10samples1Phenotype.txt ADDITIVE_LINEAR examples/testData/DELETEME -saveAsText -sampleIDName SampleID -phenoName pheno1 -overwriteParquet")
