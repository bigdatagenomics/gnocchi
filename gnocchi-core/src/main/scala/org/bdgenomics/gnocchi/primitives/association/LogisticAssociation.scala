package org.bdgenomics.gnocchi.primitives.association

case class LogisticAssociation(weights: List[Double],
                               geneticParameterStandardError: Double,
                               pValue: Double,
                               numSamples: Int) extends Association