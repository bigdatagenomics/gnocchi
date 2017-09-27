package net.fnothaft.gnocchi.primitives.association

case class LogisticAssociation(weights: List[Double],
                               geneticParameterStandardError: Double,
                               pValue: Double,
                               numSamples: Int) extends Association