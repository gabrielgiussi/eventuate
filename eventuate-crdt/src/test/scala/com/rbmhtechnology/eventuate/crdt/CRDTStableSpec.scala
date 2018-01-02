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
  val awSet = AWSet()
  val mvReg = MVRegister()
  val lwwReg = LWWRegister()
  val orShoppingCart = AWCart()

  def vt(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)

  "An AWSet" should {
    import AWSet._
    "stabilize" in {
      val updated = awSet
        .add(1, vt(1, 0))
        .stable(vt(1, 0))
      updated.value should be(Set(1))
      updated.polog.log shouldBe Set.empty
      updated.state.size shouldBe 1

    }
  }

}
