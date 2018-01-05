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

import com.rbmhtechnology.eventuate.VectorTime
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpec

class CRDTStableSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  val crdt = CRDT.zero
  val awSet = AWSet.apply[Int]

  def vt(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)

  "An AWSet" should {
    import AWSet._
    import CRDTUtils.AWSetCRDT
    "discard stable operations" in {
      val updated = awSet
        .add(1, vt(1, 0))
        .add(2, vt(2, 0))
        .add(3, vt(2, 1))
        .add(4, vt(3, 1))
        .add(5, vt(3, 2))
        .stable(vt(2, 1))
      updated.value shouldBe Set(1, 2, 3, 4, 5)
      updated.polog.log.size shouldBe 2
      updated.state.size shouldBe 3
    }
    "remove stable values" in {
      val updated = awSet
        .add(1, vt(1, 0))
        .stable(vt(1, 0))
        .remove(1, vt(2, 0))
      updated.value shouldBe Set()
    }
    "remove only stable values" in {
      val updated = awSet
        .add(1, vt(1, 0))
        .add(2, vt(2, 0))
        .remove(1, vt(2, 0))
        .stable(vt(1, 0))
      updated.value shouldBe Set(2)
    }
    "clear stable values" in {
      val updated = awSet
        .add(1, vt(1, 0))
        .add(2, vt(0, 1))
        .stable(vt(1, 0))
        .clear(vt(2, 0))
      updated.value shouldBe Set(2)
    }
  }

  "A MVRegister" should {
    import MVRegister._
    import CRDTUtils.MVRegisterCRDT
    "discard stable operations" in {
      val updated = crdt
        .assign(1, vt(1, 0))
        .assign(2, vt(0, 1))
        .stable(vt(1, 1))
      updated.value should be(Set(1, 2))
      updated.polog.log.size shouldBe 0
      updated.state.size shouldBe 2
    }
  }

  "A LWWRegister" should {
    import LWWRegister._
    import CRDTUtils.LWWRegisterCRDT
    "discard stable operations" in {
      val updated = crdt
        .assign(1, vt(1, 0), 0, "emitter1")
        .assign(2, vt(0, 1), 1, "emitter2")
        .assign(3, vt(2, 0), 2, "emitter2")
        .stable(vt(1, 1))
      updated.value should be(Some(3))
      updated.polog.log.size shouldBe 1
      updated.state.size shouldBe 1
    }
    "clear stable operations (to fix)" in {
      crdt
        .assign(1, vt(1, 0), 0, "emitter1")
        .assign(2, vt(0, 1), 1, "emitter2")
        .stable(vt(0, 1))
        .clear(vt(2, 0)) // TODO is ok this test to fail because i'm adding a ClearOP || to (0,1) and this should not happen
        .value shouldBe Some(1)
    }
    "clear stable operations" in {
      crdt
        .assign(1, vt(1, 0), 0, "emitter1")
        .assign(2, vt(0, 1), 1, "emitter2")
        .stable(vt(0, 1))
        .clear(vt(0, 2))
        .value shouldBe Some(1)
    }
  }

  "An AWCart" should {
    import AWCart._
    import CRDTUtils.AWCartCRDT
    "discard stable operations" in {
      val updated = crdt
        .add("a", 1, vt(1, 0))
        .add("b", 2, vt(2, 0))
        .add("a", 5, vt(0, 1))
        .stable(vt(1, 1))
      updated.value should be(Map("a" -> 6, "b" -> 2))
      updated.polog.log.size shouldBe 1
      updated.state.size shouldBe 2
    }
  }

}
