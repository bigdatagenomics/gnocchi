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


class GenotypeDataset(object):

    def __init__(self, jvmDS, sc):
        """
        Constructs a Python CalledVariantDataset from a JVM GenotypeDataset

        :param jvmDS: Py4j handle to underlying JVM object.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """
        self._jvmDS = jvmDS
        self.sc = sc

    def get(self):
        """
        Access the inner Dataset object
        """
        return self._jvmDS.genotypes


class LinearVariantModelDataset(object):

    def __init__(self, jvmDS, sc):
        """
        Constructs a Python LinearVariantModelDataset from a JVM Dataset[LinearVariantModel]

        :param jvmDS: Py4j handle to underlying JVM object.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """
        self._jvmDS = jvmDS
        self.sc = sc

    def get(self):
        """
        Access the inner Dataset object
        """
        return self._jvmDS


class LogisticVariantModelDataset(object):

    def __init__(self, jvmDS, sc):
        """
        Constructs a Python LogisticVariantModelDataset from a JVM Dataset[LogisticVariantModel]

        :param jvmDS: Py4j handle to underlying JVM object.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """
        self._jvmDS = jvmDS
        self.sc = sc

    def get(self):
        """
        Access the inner Dataset object
        """
        return self._jvmDS


class Phenotype(object):

    def __init__(self, p, sc):
        """
        Constructs a Python Phenotype object from a JVM Phenotype

        :param p: Py4j handle to underlying JVM object
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """
        self._jvmPhenotype = p
        self.sc = sc

class PhenotypesContainer(object):

    def __init__(self, jvmPhenoContainer, sc, jgs):
        """
        Constructs a Python PhenotypeMap from a JVM Map[String, Phenotype]

        :param jvmMap: Py4j handle to underlying JVM object.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """
        self._jvmPhenoContainer = jvmPhenoContainer
        self.sc = sc
        self._jgs = jgs

    def get(self):
        """
        Access the inner JVM Map object
        """
        return self._jvmPhenoContainer

    def getKey(self, k):
        """
        Get the Phenotype object corresponding to a specific phenotype name
        """
        p = self._jgs.getPhenotypeByKey(self._jvmPhenoContainer, k)
        return Phenotype(p, self.sc)
