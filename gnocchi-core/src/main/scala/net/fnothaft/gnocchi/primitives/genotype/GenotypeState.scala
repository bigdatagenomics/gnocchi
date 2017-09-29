package net.fnothaft.gnocchi.primitives.genotype

case class GenotypeState(sampleID: String,
                         value: String) extends Product {
  def toDouble: Double = {
    toList.filter(_ != ".").map(_.toDouble).sum
  }

  def toList: List[String] = {
    value.split("/|\\|").toList
  }
}