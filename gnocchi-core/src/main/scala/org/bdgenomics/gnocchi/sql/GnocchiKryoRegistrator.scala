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
package org.bdgenomics.gnocchi.sql

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Input, KryoDataInput, KryoDataOutput, Output }
import org.apache.hadoop.io.Writable
import org.apache.spark.serializer.KryoRegistrator
import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
import org.bdgenomics.utils.misc.Logging

/**
 * A Kryo serializer for Hadoop writables.
 *
 * Lifted from the Apache Spark user email list
 * (http://apache-spark-user-list.1001560.n3.nabble.com/Hadoop-Writable-and-Spark-serialization-td5721.html)
 * which indicates that it was originally copied from Shark itself, back when
 * Spark 0.9 was the state of the art.
 *
 * @tparam T The class to serialize, which implements the Writable interface.
 */
class WritableSerializer[T <: Writable] extends Serializer[T] {
  override def write(kryo: Kryo, output: Output, writable: T) {
    writable.write(new KryoDataOutput(output))
  }

  override def read(kryo: Kryo, input: Input, cls: java.lang.Class[T]): T = {
    val writable = cls.newInstance()
    writable.readFields(new KryoDataInput(input))
    writable
  }
}

class GnocchiKryoRegistrator extends ADAMKryoRegistrator with Logging {
  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)

    kryo.register(classOf[org.bdgenomics.gnocchi.primitives.variants.CalledVariant])
    kryo.register(classOf[org.bdgenomics.gnocchi.primitives.association.LinearAssociation])
    kryo.register(classOf[org.bdgenomics.gnocchi.primitives.association.LogisticAssociation])
    kryo.register(classOf[org.bdgenomics.gnocchi.primitives.genotype.GenotypeState])
    kryo.register(classOf[org.bdgenomics.gnocchi.primitives.phenotype.Phenotype])
    kryo.register(classOf[org.bdgenomics.gnocchi.primitives.association.LinearAssociationBuilder])

    kryo.register(classOf[org.bdgenomics.gnocchi.models.variant.LogisticVariantModel])
    kryo.register(classOf[org.bdgenomics.gnocchi.models.variant.LinearVariantModel])
  }
}
