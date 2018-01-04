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
import org.scalatest._

class CRDTSpec extends WordSpec with Matchers with BeforeAndAfterEach {
  val counter = Counter.apply[Int]
  val awSet = AWSet.apply[Int]
  val mvReg = MVRegister.apply
  val lwwReg = LWWRegister.apply
  val awShoppingCart = AWCart.apply
  val tpSet = TPSet.apply[Int]

  def vt(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)

  "A Counter" must {
    import Counter._
    "have a default value 0" in {
      counter.value shouldBe 0
    }
    "return value of single operation" in {
      counter
        .update(5, vt(1, 0))
        .value shouldBe 5
    }
    "return sum of operations" in {
      counter
        .update(6, vt(1, 0))
        .update(6, vt(2, 0))
        .value shouldBe 12
    }
    "return sum of concurrent operations" in {
      counter
        .update(6, vt(1, 0))
        .update(3, vt(0, 1))
        .value shouldBe 9
    }
    "return the sum of positive and negative operations" in {
      counter
        .update(2, vt(1, 0))
        .update(-4, vt(2, 1))
        .value shouldBe -2
    }
  }
  "An AWSet" must {
    import AWSet._
    "be empty by default" in {
      awSet.value should be('empty)
    }
    "add an entry" in {
      awSet
        .add(1, vt(1, 0))
        .value should be(Set(1))
    }
    "mask sequential duplicates" in {
      awSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))
        .value should be(Set(1))
    }
    "mask concurrent duplicates" in {
      awSet
        .add(1, vt(1, 0))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    "remove a pair" in {
      awSet
        .add(1, vt(1, 0))
        .remove(1, vt(2, 0))
        .value should be(Set())
    }
    "keep an entry if not all pairs are removed" in {
      awSet
        .add(1, vt(1, 0))
        .add(1, vt(0, 1))
        .remove(1, vt(2, 0))
        .value should be(Set(1))
    }
    "add an entry if concurrent to remove" in {
      awSet
        .add(1, vt(1, 0))
        .remove(1, vt(2, 0))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    "return an empty set after a remove" in {
      awSet
        .remove(1, vt(1, 0))
        .value should be(Set())
    }
    "return an empty set after a clear" in {
      awSet
        .clear(vt(1, 0))
        .value should be(Set())
    }
    "remove all entries after a clear" in {
      awSet
        .add(1, vt(1, 0))
        .add(2, vt(2, 0))
        .add(3, vt(0, 1))
        .clear(vt(2, 2))
        .value should be(Set())
    }
    "remove all entries in the causal past after a clear" in {
      awSet
        .add(1, vt(1, 0))
        .add(2, vt(2, 0))
        .add(3, vt(0, 1))
        .clear(vt(3, 0))
        .value should be(Set(3))
    }

  }
  "An MVRegister" must {
    import MVRegister._
    "not have set a value by default" in {
      mvReg.value should be('empty)
    }
    "store a single value" in {
      mvReg
        .assign(1, vt(1, 0))
        .value should be(Set(1))
    }
    "store multiple values in case of concurrent writes" in {
      mvReg
        .assign(1, vt(1, 0))
        .assign(2, vt(0, 1))
        .value should be(Set(1, 2))
    }
    "mask duplicate concurrent writes" in {
      mvReg
        .assign(1, vt(1, 0))
        .assign(1, vt(0, 1))
        .value should be(Set(1))
    }
    "replace a value if it happened before a new write" in {
      mvReg
        .assign(1, vt(1, 0))
        .assign(2, vt(2, 0))
        .value should be(Set(2))
    }
    "replace a value if it happened before a new write and retain a value if it is concurrent to the new write" in {
      mvReg
        .assign(1, vt(1, 0))
        .assign(2, vt(0, 1))
        .assign(3, vt(2, 0))
        .value should be(Set(2, 3))
    }
    "replace multiple concurrent values if they happened before a new write" in {
      mvReg
        .assign(1, vt(1, 0))
        .assign(2, vt(0, 1))
        .assign(3, vt(1, 1))
        .value should be(Set(3))
    }
  }
  "An LWWRegister" must {
    import LWWRegister._
    "not have a value by default" in {
      lwwReg.value should be('empty)
    }
    "store a single value" in {
      lwwReg
        .assign(1, vt(1, 0), 0, "source-1")
        .value should be(Some(1))
    }
    "accept a new value if was set after the current value according to the vector clock" in {
      lwwReg
        .assign(1, vt(1, 0), 1, "emitter-1")
        .assign(2, vt(2, 0), 0, "emitter-2")
        .value should be(Some(2))
    }

    "fallback to the wall clock if the values' vector clocks are concurrent" in {
      lwwReg
        .assign(1, vt(1, 0), 0, "emitter-1")
        .assign(2, vt(0, 1), 1, "emitter-2")
        .value should be(Some(2))
      lwwReg
        .assign(1, vt(1, 0), 1, "emitter-1")
        .assign(2, vt(0, 1), 0, "emitter-2")
        .value should be(Some(1))
    }
    "fallback to the greatest emitter if the values' vector clocks and wall clocks are concurrent" in {
      lwwReg
        .assign(1, vt(1, 0), 0, "emitter-1")
        .assign(2, vt(0, 1), 0, "emitter-2")
        .value should be(Some(2))
      lwwReg
        .assign(1, vt(1, 0), 0, "emitter-2")
        .assign(2, vt(0, 1), 0, "emitter-1")
        .value should be(Some(1))
    }
    "return none value after just a clear" in {
      lwwReg
        .clear(vt(1, 0))
        .value shouldBe None
    }
    "remove all values after a clear" in {
      lwwReg
        .assign(1, vt(1, 0), 0, "emmiter1")
        .assign(2, vt(2, 0), 1, "emmiter1")
        .assign(3, vt(0, 1), 2, "emmiter2")
        .clear(vt(2, 2))
        .value shouldBe None
    }
    "remove only values in the causal past of a clear" in {
      lwwReg
        .assign(1, vt(1, 0), 0, "emitter-1")
        .assign(2, vt(0, 1), 0, "emitter-2")
        .clear(vt(0, 2))
        .value shouldBe Some(1)
    }
  }
  "An AWCart" must {
    import AWCart._
    "be empty by default" in {
      awShoppingCart.value should be('empty)
    }
    "set initial entry quantities" in {
      awShoppingCart
        .add("a", 2, vt(1, 0))
        .add("b", 3, vt(2, 0))
        .value should be(Map("a" -> 2, "b" -> 3))
    }
    "increment existing entry quantities" in {
      awShoppingCart
        .add("a", 1, vt(1, 0))
        .add("b", 3, vt(2, 0))
        .add("a", 1, vt(3, 0))
        .add("b", 1, vt(4, 0))
        .value should be(Map("a" -> 2, "b" -> 4))
    }
    "remove observed entries" in {
      awShoppingCart
        .add("a", 2, vt(1, 0))
        .add("b", 3, vt(2, 0))
        .add("a", 1, vt(3, 0))
        .remove("b", vt(4, 0))
        .add("b", 1, vt(5, 0))
        .remove("a", vt(6, 0))
        .value should be(Map("b" -> 1))
    }
    "not remove entries if remove is concurrent" in {
      awShoppingCart
        .add("a", 2, vt(1, 0))
        .remove("a", vt(0, 1))
        .value should be(Map("a" -> 2))
    }
    "remove only entries in the causal past" in {
      awShoppingCart
        .add("a", 2, vt(1, 0))
        .add("a", 2, vt(3, 0))
        .remove("a", vt(1, 1))
        .value should be(Map("a" -> 2))
    }
    "return empty after just a clear" in {
      awShoppingCart
        .clear(vt(1, 0))
        .value should be('empty)
    }
    "remove all entries after just a clear" in {
      awShoppingCart
        .add("a", 1, vt(1, 0))
        .add("b", 2, vt(2, 0))
        .add("a", 1, vt(0, 1))
        .clear(vt(2, 2))
        .value should be('empty)
    }
    "remove all entries in the causal past after a clear" in {
      awShoppingCart
        .add("a", 1, vt(1, 0))
        .add("b", 2, vt(0, 1))
        .add("a", 1, vt(2, 0))
        .clear(vt(1, 2))
        .value should be(Map("a" -> 1))
    }

  }

  "A TPSet" must {
    import TPSet._
    "be empty by default" in {
      tpSet.value shouldBe Set.empty
    }
    "add an entry" in {
      tpSet
        .add(1, vt(1, 0))
        .value should be(Set(1))
    }
    "mask sequential duplicates" in {
      tpSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))
        .value should be(Set(1))
    }
    "mask concurrent duplicates" in {
      tpSet
        .add(1, vt(1, 0))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    "remove a pair" in {
      tpSet
        .add(1, vt(1, 0))
        .remove(1, vt(2, 0))
        .value should be(Set())
    }
    "don't add an element that was removed" in {
      tpSet
        .add(1, vt(1, 0))
        .remove(1, vt(0, 1))
        .add(1, vt(2, 1))
        .value should be(Set())
    }
    "commute" in { // TODO rename!
      tpSet
        .add(1, vt(1, 0))
        .remove(1, vt(0, 1))
        .value should be(Set())
    }
    "commute2" in {
      tpSet
        .remove(1, vt(0, 1))
        .add(1, vt(1, 0))
        .value should be(Set())
    }
    "return an empty set after a remove" in {
      tpSet
        .remove(1, vt(1, 0))
        .value should be(Set())
    }

  }

}
