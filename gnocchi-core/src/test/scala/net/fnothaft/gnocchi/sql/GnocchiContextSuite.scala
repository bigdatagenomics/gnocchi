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

package net.fnothaft.gnocchi.sql

import net.fnothaft.gnocchi.GnocchiFunSuite
import net.fnothaft.gnocchi.sql.GnocchiContext._
import org.apache.spark.sql.SparkSession
import net.fnothaft.gnocchi.sql.GnocchiContext._

class GnocchiContextSuite extends GnocchiFunSuite {

  ignore("Test loadAndFilterGenotypes: load VCF with no filters; assert that all genotypes present") {

  }

  ignore("Test loadAndFilterGenotypes: load VCF with maf filter; ") {

  }
  ignore("Test loadAndFilterGenotypes: output from vcf input") {
    //    val genoFilePath = ClassLoader.getSystemClassLoader.getResource("small1.vcf").getFile
    //    val genotypeStateDataset = sc.loadAndFilterGenotypes(genoFilePath,
    //      adamDestination, ploidy, mind, maf, geno, overwrite)
    //    val genotypeStateArray = genotypeStateDataset.collect()
    //    val genotypeState = genotypeStateArray(0)
    //    assert(genotypeState.start === 14521, "GenotypeState start is incorrect: " + genotypeState.start)
    //    assert(genotypeState.end === 14522, "GenotypeState end is incorrect: " + genotypeState.end)
    //    assert(genotypeState.ref === "G", "GenotypeState ref is incorrect: " + genotypeState.ref)
    //    assert(genotypeState.alt === "A", "GenotypeState alt is incorrect: " + genotypeState.alt)
    //    assert(genotypeState.sampleId === "sample1", "GenotypeState sampleId is incorrect: " + genotypeState.sampleId)
    //    assert(genotypeState.genotypeState === 1, "GenotypeState genotypeState is incorrect: " + genotypeState.genotypeState)
  }
}
