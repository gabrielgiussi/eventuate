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

package com.rbmhtechnology.eventuate.log

import org.scalatest.Matchers
import org.scalatest.WordSpec

class EventLogMembershipSpec extends WordSpec with Matchers {

  import EventLogMembershipProtocol._

  /**
   *      A - C
   *     /     \
   *    B       D
   */
  val partitionA = UnconnectedPartition("A", Set("B", "C"))
  val partitionB = UnconnectedPartition("B", Set("A"))
  val partitionC = UnconnectedPartition("C", Set("A", "D"))
  val partitionD = UnconnectedPartition("D", Set("C"))

  implicit def asInitialPartition(unconnected: UnconnectedPartition) = InitialPartitions(unconnected.logId, unconnected.neighbors)

  "a" must {
    "b" in {
      val state0 = EventLogMembershipState(partitionA)
      state0 should be(EventLogMembershipState(Set("A", "B", "C"), Set("A"), Set("B", "C"), Set()))
      val state1 = processPartition(state0)(partitionB)
      state1 should be(EventLogMembershipState(Set("A", "B", "C"), Set("A", "B"), Set("C"), Set()))
      val state2 = processPartition(state1)(partitionC)
      state2 should be(EventLogMembershipState(Set("A", "B", "C", "D"), Set("A", "B", "C"), Set("D"), Set()))
      val state3 = processPartition(state2)(partitionD)
      state3 should be(EventLogMembershipState(Set("A", "B", "C", "D"), Set("A", "B", "C", "D"), Set(), Set()))
    }
    "c" in {
      val state0 = EventLogMembershipState(partitionA)
      state0 should be(EventLogMembershipState(Set("A", "B", "C"), Set("A"), Set("B", "C"), Set()))
      val state1 = processPartition(state0)(partitionB)
      state1 should be(EventLogMembershipState(Set("A", "B", "C"), Set("A", "B"), Set("C"), Set()))
      val state2 = processPartition(state1)(partitionD)
      state2 should be(EventLogMembershipState(Set("A", "B", "C"), Set("A", "B"), Set("C"), Set(UnconnectedPartition("D", Set("C")))))
      val state3 = processPartition(state2)(partitionC)
      state3 should be(EventLogMembershipState(Set("A", "B", "C", "D"), Set("A", "B", "C", "D"), Set(), Set()))
    }
  }
}
