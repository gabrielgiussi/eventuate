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

package com.rbmhtechnology.eventuate.crdt

import org.scalatest.{ BeforeAndAfterEach, Matchers, WordSpec }

class CRDTOpBasedSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  /*

  case object ObsoleteAll

  case object ObsoleteCausalDependent

  case class FakeOp(v: String)

  def vt(t1: Long, t2: Long): VectorTime = VectorTime("p1" -> t1, "p2" -> t2)

  def expected(stable: String, evaluated: String) = s"stable: $stable, evaluated: $evaluated"

  implicit val ops = new CRDTNonCommutativePureOp[String] {

    override implicit def obs: Obsolete = {
      case (_, Versioned(ObsoleteAll, _, _, _)) => true
      case (Versioned(FakeOp, t1, _, _), Versioned(ObsoleteCausalDependent, t2, _, _)) if t1 < t2 => true
      case _ => false
    }

    override def zero: CRDT[String] = CRDT("initial")

    override protected def customEval(crdt: CRDT[String]): String = ???

  }

  "A Pure OP Based CRDT" ignore {
    "return the merge of the stable state and the evaluated state" in {
      ops.zero.value shouldBe expected("initial", "empty")
    }
    "" in {
    }
  }
*/
}
