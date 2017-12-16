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
  val counter: Counter[Int] = Counter.apply[Int]
  val orSet = ORSet[Int]()
  val mvReg = MVRegister[Int]()
  val lwwReg = LWWRegister[Int]()
  val orShoppingCart = ORCart[String]()

  def vt(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)

  "A Counter" must {
    "have a default value 0" in {
      counter.eval shouldBe 0
    }
    "return value of single operation" in {
      counter
        .addOp(UpdateOp(5), vt(1, 0))
        .eval shouldBe 5
    }
    "return sum of operations" in {
      counter
        .addOp(UpdateOp(6), vt(1, 0))
        .addOp(UpdateOp(6), vt(2, 0))
        .eval shouldBe 12
    }
    "return sum of concurrent operations" in {
      counter
        .addOp(UpdateOp(6), vt(1, 0))
        .addOp(UpdateOp(3), vt(0, 1))
        .eval shouldBe 9
    }
    "return the sum of positive and negative operations" in {
      counter
        .addOp(UpdateOp(2), vt(1, 0))
        .addOp(UpdateOp(-4), vt(2, 1))
        .eval shouldBe -2
    }
  }

  "An ORSet" must {
    "be empty by default" in {
      orSet.value should be('empty)
    }
    "add an entry" in {
      orSet
        .add(1, vt(1, 0))
        .eval should be(Set(1))
    }
    "mask sequential duplicates" in {
      orSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))
        .value should be(Set(1))
    }
    "mask concurrent duplicates" in {
      orSet
        .add(1, vt(1, 0))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    "remove a pair" in {
      orSet
        .add(1, vt(1, 0))
        .remove(1, vt(2, 0)) //.remove(1,Set(vt(1, 0)))
        .value should be(Set())
    }
    /*
    "remove an entry by removing all pairs" in {
      val tmp = orSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))

      tmp
        .remove(tmp.prepareRemove(1))
        .value should be(Set())
    }
    */
    "keep an entry if not all pairs are removed" in {
      orSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))
        .remove(1, vt(1, 0)) //.remove(Set(vt(1, 0)))
        .value should be(Set(1))
    }
    "add an entry if concurrent to remove" in {
      orSet
        .add(1, vt(1, 0))
        .remove(1, vt(1, 0)) //.remove(Set(vt(1, 0)))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    /*
    "prepare a remove-set if it contains the given entry" in {
      orSet
        .add(1, vt(1, 0))
        .prepareRemove(1) should be(Set(vt(1, 0)))
    }
    "not prepare a remove-set if it doesn't contain the given entry" in {
      orSet.prepareRemove(1) should be('empty)
    }
    */
  }

  "An MVRegister" must {
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

    "An LWWRegister" must {
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

      "An ORCart" must {
        "be empty by default" in {
          orShoppingCart.value should be('empty)
        }
        "set initial entry quantities" in {
          orShoppingCart
            .add("a", 2, vt(1, 0))
            .add("b", 3, vt(2, 0))
            .value should be(Map("a" -> 2, "b" -> 3))
        }
        "increment existing entry quantities" in {
          orShoppingCart
            .add("a", 1, vt(1, 0))
            .add("b", 3, vt(2, 0))
            .add("a", 1, vt(3, 0))
            .add("b", 1, vt(4, 0))
            .value should be(Map("a" -> 2, "b" -> 4))
        }
        "remove observed entries" in {
          orShoppingCart
            .add("a", 2, vt(1, 0))
            .add("b", 3, vt(2, 0))
            .add("a", 1, vt(3, 0))
            .add("b", 1, vt(4, 0))
            //.remove(Set(vt(1, 0), vt(2, 0), vt(3, 0))) TODO what means observed remove?
            .remove("b", 3, vt(3, 0)) // FIXME para que esto funcione deberia estar mandando Versioned(b,3,VectorTime(2,0))
            .remove("a", 1, vt(4, 0)) // before the a with vt(3,0) was removed // TODO this doesn't work
            .value should be(Map("b" -> 1))
        }
      }
    }
  }

}
/*
class CRDTSpec extends WordSpec with Matchers with BeforeAndAfterEach {

  "An ORSet" must {
    "be empty by default" in {
      orSet.value should be('empty)
    }
    "add an entry" in {
      orSet
        .add(1, vt(1, 0))
        .value should be(Set(1))
    }
    "mask sequential duplicates" in {
      orSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))
        .value should be(Set(1))
    }
    "mask concurrent duplicates" in {
      orSet
        .add(1, vt(1, 0))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    "remove a pair" in {
      orSet
        .add(1, vt(1, 0))
        .remove(Set(vt(1, 0)))
        .value should be(Set())
    }
    "remove an entry by removing all pairs" in {
      val tmp = orSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))

      tmp
        .remove(tmp.prepareRemove(1))
        .value should be(Set())
    }
    "keep an entry if not all pairs are removed" in {
      orSet
        .add(1, vt(1, 0))
        .add(1, vt(2, 0))
        .remove(Set(vt(1, 0)))
        .value should be(Set(1))
    }
    "add an entry if concurrent to remove" in {
      orSet
        .add(1, vt(1, 0))
        .remove(Set(vt(1, 0)))
        .add(1, vt(0, 1))
        .value should be(Set(1))
    }
    "prepare a remove-set if it contains the given entry" in {
      orSet
        .add(1, vt(1, 0))
        .prepareRemove(1) should be(Set(vt(1, 0)))
    }
    "not prepare a remove-set if it doesn't contain the given entry" in {
      orSet.prepareRemove(1) should be('empty)
    }
  }


}
*/
