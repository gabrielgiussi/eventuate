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
  val orSet = ORSet[Int]()

  val orSetOps = ORSet.ORSetServiceOps[Int]

  def vectorTime(t1: Long, t2: Long): VectorTime =
    VectorTime("p1" -> t1, "p2" -> t2)



  "An ORSet" must {
    "be empty by default" in {
      orSet.eval should be('empty)
    }
    "add an entry" in {
      orSetOps
        .effect(orSet,AddOp(1),vectorTime(1, 0))
        .eval should be(Set(1))
    }/*
    "mask sequential duplicates" in {
      orSetOps
        .effect(orSet,AddOp(1),vectorTime(1, 0))
      orSetOps
        .effect(orSet,AddOp(1),vectorTime(2, 0))
        .value should be(Set(1))
    }
    "mask concurrent duplicates" in {
      orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(0, 1))
        .value should be(Set(1))
    }
    "remove a pair" in {
      orSet
        .add(1, vectorTime(1, 0))
        .remove(Set(vectorTime(1, 0)))
        .value should be(Set())
    }
    "remove an entry by removing all pairs" in {
      val tmp = orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(2, 0))

      tmp
        .remove(tmp.prepareRemove(1))
        .value should be(Set())
    }
    "keep an entry if not all pairs are removed" in {
      orSet
        .add(1, vectorTime(1, 0))
        .add(1, vectorTime(2, 0))
        .remove(Set(vectorTime(1, 0)))
        .value should be(Set(1))
    }
    "add an entry if concurrent to remove" in {
      orSet
        .add(1, vectorTime(1, 0))
        .remove(Set(vectorTime(1, 0)))
        .add(1, vectorTime(0, 1))
        .value should be(Set(1))
    }
    "prepare a remove-set if it contains the given entry" in {
      orSet
        .add(1, vectorTime(1, 0))
        .prepareRemove(1) should be(Set(vectorTime(1, 0)))
    }
    "not prepare a remove-set if it doesn't contain the given entry" in {
      orSet.prepareRemove(1) should be('empty)
    }*/
  }

}
