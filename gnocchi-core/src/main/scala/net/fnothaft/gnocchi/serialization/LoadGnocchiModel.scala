//import java.io.FileInputStream
//
//import com.esotericsoftware.kryo.Kryo
//import com.esotericsoftware.kryo.io.Input
//import net.fnothaft.gnocchi.models.{GnocchiModel, AdditiveLogisticGnocchiModel}
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
//object LoadGnocchiModel = {
//	def apply(inputFileName: String,
//						modelType: String): GnocchiModel = {
//		val kryo: Kryo = new Kryo()
//		val registrator: ADAMKryoRegistrator = new ADAMKryoRegistrator()
//		registrator.registerClasses(kryo)
//		val input: Input = new Input(new FileInputStream(inputFileName))
//		// need to load models differently based on what kind of model it is.
//		match modelType {
//			case "AdditiveLogisticGnocchiModel" => {
//				val model: GnocchiModel = kryo.readObject(input, "string")
//			}
//			case _ => {
//				val model: GnocchiModel = kryo.readObject(input, AdditiveLogisticGnocchiModel)
//			}
//
//		}
//		input.close()
//		model
//	}
//}
//
