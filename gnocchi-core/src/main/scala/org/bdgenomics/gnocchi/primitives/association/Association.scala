package org.bdgenomics.gnocchi.primitives.association

trait Association {
  val weights: List[Double]
  val geneticParameterStandardError: Double
  val pValue: Double
  val numSamples: Int
}
