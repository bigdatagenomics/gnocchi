//import java.io.FileOutputStream
//
//import com.esotericsoftware.kryo.Kryo
//import com.esotericsoftware.kryo.io.Output
//import net.fnothaft.gnocchi.models.GnocchiModel
//import org.bdgenomics.adam.serialization.ADAMKryoRegistrator
//
///**
// * Copyright 2016 Taner Dagdelen
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//object SaveGnocchiModel {
//  def apply(model: GnocchiModel,
//            outputFileName: String): Unit = {
//    val kryo: Kryo = new Kryo()
//    val registrator: ADAMKryoRegistrator = new ADAMKryoRegistrator()
//    registrator.registerClasses(kryo)
//    val output: Output = new Output(new FileOutputStream(outputFileName))
//    kryo.writeObject(output, model)
//    output.close()
//  }
//}
//
