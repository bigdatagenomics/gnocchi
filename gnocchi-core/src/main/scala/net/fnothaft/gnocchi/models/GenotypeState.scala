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
package net.fnothaft.gnocchi.models

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, Variant }

case class GenotypeState(contig: String,
                         start: Long,
                         end: Long,
                         ref: String,
                         alt: String,
                         sampleId: String,
                         genotypeState: Int,
                         missingGenotypes: Int) {

  def referenceAllele: (ReferenceRegion, String) = {
    (ReferenceRegion(contig, start, end), alt)
  }

  def variant: Variant = {
    Variant.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName(contig)
        .build())
      .setStart(start)
      .setEnd(end)
      .setReferenceAllele(ref)
      .setAlternateAllele(alt)
      .build()
  }

  def tempVariantId = {
    val posString = start.toString
    s"$posString" + "_$alt"
  }
}
