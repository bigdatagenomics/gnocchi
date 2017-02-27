/**
 * Copyright 2015 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.fnothaft.gnocchi.imputation

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.formats.avro.{ Genotype, GenotypeAllele }
import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[gnocchi] object FillGenotypes extends Serializable {

  def fillInVC(vc: VariantContext,
               samples: Set[String],
               fillIn: Seq[GenotypeAllele]): Iterable[Genotype] = {
    @tailrec def decimateSamples(iter: Iterator[Genotype],
                                 sampleSet: Set[String]): Set[String] = {
      if (!iter.hasNext) {
        sampleSet
      } else {
        decimateSamples(iter, sampleSet - iter.next.getSampleId)
      }
    }

    // what samples are missing at this site?
    val missingSamples = decimateSamples(vc.genotypes.toIterator,
      samples)

    // for these samples, create new genotypes
    val filledInGenotypes = missingSamples.toIterable
      .map(s => {
        Genotype.newBuilder
          .setVariant(vc.variant.variant)
          .setSampleId(s)
          .setAlleles(fillIn.asJava)
          .build()
      })

    vc.genotypes ++ filledInGenotypes
  }

  def apply(rdd: RDD[Genotype],
            useNoCall: Boolean = false,
            ploidy: Int = 2): RDD[Genotype] = {

    // create list for filling in sites
    val fillIn = Seq.fill(ploidy)({
      if (useNoCall) {
        GenotypeAllele.NoCall
      } else {
        GenotypeAllele.Ref
      }
    })

    // get sample ids
    val sampleIds = rdd.map(_.getSampleId)
      .distinct
      .collect
      .toSet

    // transform genotypes into variant contexts
    val vcRdd = rdd.toVariantContext

    // flat map to fill in
    vcRdd.flatMap(fillInVC(_, sampleIds, fillIn))
  }
}
