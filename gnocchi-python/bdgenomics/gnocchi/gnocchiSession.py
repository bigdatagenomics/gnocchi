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

from bdgenomics.gnocchi.primitives import CalledVariantDataset, PhenotypeMap
from py4j.java_collections import ListConverter

class GnocchiSession(object):
    """
    The GnocchiSession provides functions on top of a SparkContext for loading
    and analyzing genome data.
    """

    def __init__(self, ss):
        """
        Initializes a GnocchiSession using a SparkSession.

        :param pyspark.sql.SparkSession ss: The currently active
        SparkSession.
        """
        self._sc = ss.sparkContext
        self._jvm = self._sc._jvm
        session = self._jvm.org.bdgenomics.gnocchi.sql.GnocchiSession.GnocchiSessionFromSession(ss._jsparkSession)
        self.__jgs = self._jvm.org.bdgenomics.gnocchi.api.java.JavaGnocchiSession(session)


    def filterSamples(self, genotypesDataset, mind, ploidy):
        """
        Returns a filtered Dataset of CalledVariant objects, where all values
        with fewer samples than the mind threshold are filtered out.

        :param bdgenomics.gnocchi.primitives.CalledVariantDataset genotypesDataset:
               the dataset of CalledVariant objects to filter on.
        :param float mind: the percentage threshold of samples to have filled in;
               values with fewer samples will be removed in this operation.
        :param float ploidy: the number of sets of chromosomes.
        :return: a filtered Dataset of CalledVariant objects
        :rtype: bdgenomics.gnocchi.primitives.CalledVariantDataset
        """
        dataset = self.__jgs.filterSamples(genotypesDataset.get(), mind, ploidy)
        return CalledVariantDataset(dataset, self._sc)


    def filterVariants(self, genotypesDataset, geno, maf):
        """
        Returns a filtered Dataset of CalledVariant objects, where all variants
        with values less than the specified geno or maf threshold are filtered 
        out.

        :param bdgenomics.gnocchi.primitives.CalledVariantDataset genotypesDataset:
               the dataset of CalledVariant objects to filter on.
        :param float geno: the percentage threshold for geno values for each
               CalledVariant object.
        :param float maf: the percentage threshold for Minor Allele Frequency
               for each CalledVariant object.
        :return: a filtered Dataset of CalledVariant objects
        :rtype: bdgenomics.gnocchi.primitives.CalledVariantDataset
        """
        dataset = self.__jgs.filterVariants(genotypesDataset.get(), geno, maf)
        return CalledVariantDataset(dataset, self._sc)


    def recodeMajorAllele(self, genotypesDataset):
        """
        Returns a modified Dataset of CalledVariant objects, where any value
        with a maf > 0.5 is recoded. The recoding is specified as flipping the
        referenceAllele and alternateAllele when the frequency of alt is greater
        than that of ref.

        :param bdgenomics.gnocchi.primitives.CalledVariantDataset genotypesDataset:
               the dataset of CalledVariant objects to recode.
        :return: a filtered Dataset of CalledVariant objects
        :rtype: bdgenomics.gnocchi.primitives.CalledVariantDataset
        """
        dataset = self.__jgs.recodeMajorAllele(genotypesDataset.get())
        return CalledVariantDataset(dataset, self._sc)


    def loadGenotypes(self, genotypesPath):
        """
        Loads a Dataset of CalledVariant objects from a file.

        :param string genotypesPath: A string specifying the location in the
               file system of the genotypes file to load in.
        :return: a Dataset of CalledVariant objects loaded from a vcf file
        :rtype: bdgenomics.gnocchi.primitives.CalledVariantDataset
        """
        dataset = self.__jgs.loadGenotypes(genotypesPath)
        return CalledVariantDataset(dataset, self._sc)


    def loadPhenotypes(self,
                       phenotypesPath,
                       primaryID,
                       phenoName,
                       delimiter,
                       covarPath=None,
                       covarNames=None,
                       covarDelimiter="\t",
                       missing=[-9]):
        """
        Returns a map of phenotype name to phenotype object, which is loaded
        from a file, specified by phenotypesPath.

        :param string phenotypesPath: A string specifying the location in the
               file system of the phenotypes file to load in.
        :param string primaryID: The primary sample ID
        :param string phenoName: The primary phenotype
        :param string delimiter: The delimiter used in the input file
        :param string covarPath: Optional parameter specifying the location in
               the file system of the covariants file
        :param list[string] covarNames: Optional paramter specifying the names
               of the covariants detailed in the covariants file
        :param string covarDelimiter: The delimiter used in the covariants file
        :return A map of phenotype name to phenotype object
        :rtype: bdgenomics.gnocchi.primitives.PhenotypeMap
        """

        if covarNames:
            covarNames = ListConverter().convert(covarNames, self._sc._gateway._gateway_client)

        missing = ListConverter().convert(missing, self._sc._gateway._gateway_client)

        phenoMap = self.__jgs.loadPhenotypes(phenotypesPath,
                                             primaryID,
                                             phenoName,
                                             delimiter,
                                             covarPath,
                                             covarNames,
                                             covarDelimiter,
                                             missing)

        return PhenotypeMap(phenoMap, self._sc, self.__jgs)
