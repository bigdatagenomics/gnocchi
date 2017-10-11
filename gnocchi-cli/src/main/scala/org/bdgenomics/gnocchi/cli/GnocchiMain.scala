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
package org.bdgenomics.gnocchi.cli

object GnocchiMain {

  private val commands = List(
    RegressPhenotypes)

  private def printCommands() {
    println("\n")
    println("\nUsage: gnocchi-submit [<spark-args> --] <gnocchi-args>")
    println("\nChoose one of the following commands:")
    commands.foreach(cmd => {
      println("%20s : %s".format(cmd.commandName, cmd.commandDescription))
    })
    println("\n")
  }

  def main(args: Array[String]) {
    println("ADAM invoked with args: %s".format(args.mkString(" ")))
    if (args.size < 1) {
      printCommands()
    } else {
      commands.find(_.commandName == args(0)) match {
        case None => printCommands()
        case Some(cmd) =>
          cmd.apply(args drop 1).run()
      }
    }
  }
}
