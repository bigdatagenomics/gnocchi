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

from bdgenomics.gnocchi.primitives import CalledVariantDataset, LogisticVariantModelDataset
from py4j.java_collections import ListConverter

class LogisticGnocchiModel(object):

    def __init__(self, ss, jlgm):
        self._ss = ss
        self._sc = ss.sparkContext
        self._jvm = self._sc._jvm
        self._jlgm = jlgm

    @classmethod
    def New(cls,
            ss,
            genotypes,
            phenotypes,
            phenotypeNames,
            QCVariantIDs=None,
            QCVariantSamplingRate=0.1,
            allelicAssumption="ADDITIVE",
            validationStringency="STRICT"):

        sc = ss.sparkContext
        jvm = sc._jvm
        jlgmf = jvm.org.bdgenomics.gnocchi.api.java.JavaLogisticGnocchiModelFactory
        session = jvm.org.bdgenomics.gnocchi.sql.GnocchiSession.GnocchiSessionFromSession(ss._jsparkSession)
        jlgmf.generate(session)

        if phenotypeNames:
            phenotypeNames = ListConverter().convert(phenotypeNames, sc._gateway._gateway_client)
        if QCVariantIDs:
            QCVariantIDs = ListConverter().convert(QCVariantIDs, sc._gateway._gateway_client)

        lgm = jlgmf.apply(genotypes.get(),
                          phenotypes.get(),
                          phenotypeNames,
                          QCVariantIDs,
                          QCVariantSamplingRate,
                          allelicAssumption,
                          validationStringency)

        jlgm = jvm.org.bdgenomics.gnocchi.api.java.JavaLogisticGnocchiModel(lgm)

        return cls(ss, jlgm)

    def get(self):
        return self.__jlgm

    def mergeGnocchiModel(self, otherModel):
        newModel = self.__jlgm.mergeGnocchiModel(otherModel.get())
        return LogisticGnocchiModel(self._ss, newModel)

    def mergeVariantModels(self, newVariantModels):
        dataset = self.__jlgm.mergeVariantModels(newVariantModels.get())
        return LogisticVariantModelDataset(dataset, self._sc)

    def mergeQCVariants(self, newQCVariantModels):
        dataset = self.__jlgm.mergeQCVariants(newQCVariantModels.get())
        return CalledVariantDataset(dataset, self._sc)

    def getVariantModels(self):
        dataset = self._jlgm.getVariantModels()
        return LogisticVariantModelDataset(dataset, self._sc)

    def getQCVariants(self):
        dataset = self._jlgm.getQCVariants()
        return CalledVariantDataset(dataset, self._sc)

    def getModelMetadata(self):
        return self._jlgm.getModelMetadata()
    
    def getModelType(self):
        return self._jlgm.getModelType()
    
    def getPhenotype(self):
        return self._jlgm.getPhenotype()
    
    def getCovariates(self):
        return self._jlgm.getCovariates()
    
    def getNumSamples(self):
        return self._jlgm.getNumSamples()
    
    def getHaplotypeBlockErrorThreshold(self):
        return self._jlgm.getHaplotypeBlockErrorThreshold()
    
    def getFlaggedVariantModels(self):
        return self._jlgm.getFlaggedVariantModels()
        
    def save(self, saveTo):
        self._jlgm.save(saveTo)
