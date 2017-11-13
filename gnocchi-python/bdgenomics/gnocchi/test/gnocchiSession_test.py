#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from bdgenomics.gnocchi.gnocchiSession import GnocchiSession
from bdgenomics.gnocchi.test import SparkTestCase


class GnocchiSessionTest(SparkTestCase):

    def test_load_genotypes(self):

        testFile = self.resourceFile("1Sample1Variant.vcf")
        gs = GnocchiSession(self.ss)

        genotypes = gs.loadGenotypes(testFile)

        self.assertEqual(genotypes._jvmDS.toJavaRDD().count(), 1)


    def test_load_phenotypes(self):

        testFile = self.resourceFile("first5samples5phenotypes2covars.txt")
        gs = GnocchiSession(self.ss)

        phenoMap = gs.loadPhenotypes(testFile, "SampleID", "pheno1", "\t",
                                     testFile, ["pheno4", "pheno5"], "\t")

        self.assertEqual(phenoMap._jvmMap.size(), 5)


    def test_filters_integration(self):

        testFile = self.exampleFile("time_genos_1.vcf")
        gs = GnocchiSession(self.ss)

        genotypes = gs.loadGenotypes(testFile)
        self.assertEqual(genotypes._jvmDS.toJavaRDD().count(), 10000)

        genotypes = gs.filterSamples(genotypes, 0.1, 2)
        genotypes = gs.filterVariants(genotypes, 0.1, 0.1)

        self.assertEqual(genotypes._jvmDS.toJavaRDD().count(), 8297)
