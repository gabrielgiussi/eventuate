/*
 * Copyright 2015 - 2016 Red Bull Media House GmbH <http://www.redbullmediahouse.com> - all rights reserved.
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

package com.rbmhtechnology.eventuate

import com.rbmhtechnology.eventuate.StabilityChecker.StabilityCheck
import org.scalatest.WordSpecLike

object StabilityCheckerTest {

  val nodeNames = List("A", "B", "C", "D", "E", "F")

  implicit class EnhancedVectorTime(vt: VectorTime) {
    def stable(implicit matrix: Seq[VectorTime], stabilityCheck: StabilityCheck): Boolean = stabilityCheck(matrix)(vt)

    def noStable(implicit matrix: Seq[VectorTime], stabilityCheck: StabilityCheck): Boolean = !stable
  }

  implicit class StringToTimestamp(val sc: StringContext) {
    private def stringToPair(s: String): (String, Long) = s.split("=").map(_.trim).toList match {
      case name :: number :: Nil => name -> number.toLong
      case _                     => throw new RuntimeException("Wrong matrix format")
    }

    private def stringToVectorTimeWithNames(s: String): VectorTime = VectorTime(s.split(",").toSeq.map(stringToPair): _*)
    private def stringToVectorTime(s: String): VectorTime = VectorTime(nodeNames.zip(s.split(",").map(_.trim.toLong).toList): _*)

    def $(): Seq[VectorTime] = { // TODO refactor
      val lines = sc.s().stripMargin.trim.split(System.lineSeparator()).toSeq.map(_.trim)
      lines.map(stringToVectorTime)
    }

    def $$(): VectorTime = stringToVectorTime(sc.s().stripMargin.trim)
  }

}

class StabilityCheckerTest extends WordSpecLike {

  import StabilityCheckerTest._
  import StabilityChecker._

  implicit val stc = stabilityCheck

  // TODO remover la necesidad de los nombres de los nodos
  /*
  "Probando" in {
    implicit val matrix =
      $"""
         |A = 2,B = 1
         |A = 2, B = 1
      """
    assert($$"A = 2, B = 1".stable)
    assert($$"A = 2, B = 2".noStable)
    assert($$"A = 1, B = 1".stable)
    assert($$"A = 3, B = 3".noStable)
  }
  "Georges Younes" in {
    implicit val matrix =
      $"""
         |A = 2, B = 1, C = 1
         |A = 0, B = 1, C = 0
         |A = 0, B = 0, C = 1
      """
    assert($$"A = 1, B = 0, C = 0".noStable)
    assert($$"A = 2, B = 0, C = 0".noStable)
    assert($$"A = 0, B = 0, C = 1".noStable)
    assert($$"A = 0, B = 1, C = 0".noStable)
  }
  */
  "Georges Younes" in {
    implicit val matrix =
      $"""
         |2, 1, 1
         |0, 1, 0
         |0, 0, 1
      """
    assert($$"1, 0, 0".noStable)
    assert($$"2, 0, 0".noStable)
    assert($$"0, 0, 1".noStable)
    assert($$"0, 1, 0".noStable)
  }

}
