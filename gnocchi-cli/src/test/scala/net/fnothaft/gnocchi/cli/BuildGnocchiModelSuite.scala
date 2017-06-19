/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi.cli

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.models.GnocchiModelMetaData
import net.fnothaft.gnocchi.rdd.phenotype.Phenotype

class BuildGnocchiModelSuite extends GnocchiFunSuite {

  sparkTest("BuildMetaData should return GnocchiModelMetaData object with correct fields") {
    val phenos = sc.parallelize(List(Phenotype("Pheno Name", "sample1", Array[Double]()), Phenotype("Pheno Name", "sample1", Array[Double]())))
    val numSamples = 2
    val errorThresh = 0.2
    val modelType = "ADDITIVE_LINEAR"
    val variables = "var1,var2"
    val flaggedVariantModels = List[String]()
    val phenotype = "phenoName"
    val metaData = new GnocchiModelMetaData(numSamples, errorThresh, modelType, variables, flaggedVariantModels, phenotype)
    val buildGMArgs = new BuildGnocchiModelArgs
    val buildGM = new BuildGnocchiModel(buildGMArgs)
    val compMetaData = buildGM.buildMetaData(phenos, errorThresh, modelType, Option(variables), phenotype)
    assert(metaData.haplotypeBlockErrorThreshold == compMetaData.haplotypeBlockErrorThreshold,
      "Error threshold from buildMetaData did not match manually constructed GnocchiModelMetaData")
    assert(metaData.modelType == compMetaData.modelType,
      "Model type from buildMetaData did not match manually constructed GnocchiModelMetaData")
    assert(metaData.flaggedVariantModels == compMetaData.flaggedVariantModels,
      "Flagged models from buildMetaData did not match manually constructed GnocchiModelMetaData")
    assert(metaData.numSamples == compMetaData.numSamples,
      "Num samples from buildMetaData did not match manually constructed GnocchiModelMetaData")
    assert(metaData.phenotype == compMetaData.phenotype,
      "Phenotype from buildMetaData did not match manually constructed GnocchiModelMetaData")
    assert(metaData.variables == compMetaData.variables,
      "Variables from buildMetaData did not match manually constructed GnocchiModelMetaData")
  }

  ignore("SelectComparisonModels should select first variant from each haplotype block") {

  }

  ignore("loaded model should have correct type: AdditiveLogisticGnocchiModel") {

  }

  ignore("loaded model should have correct type: DominantLogisticGnocchiModel") {

  }

  ignore("loaded model should have correct type: AdditiveLinearGnocchiModel") {

  }

  ignore("loaded model should have correct type: DominantLinearGnocchiModel") {

  }

}
