package net.fnothaft.gnocchi.primitives.genotype

case class GenotypeState(sampleID: String,
                         value: String) extends Product {
  def toDouble: Double = {
    value.split("/|\\|").filter(_ != ".").map(_.toDouble).sum
  }
}