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

  import scala.language.implicitConversions

  implicit def asInitialPartition(unconnected: UnconnectedPartition) = InitialPartitions(unconnected.logId, unconnected.neighbors)

  // TODO this test doesn't capture the initial communication of endpoints that may or not be part of the eventlog

  "Event Log" must {
    "solve membership when there are no more unknown partitions" in {
      val state0 = EventLogMembershipState(partitionA)
      state0 should be(EventLogMembershipState(Set("A"), Set("B", "C"), Set()))
      val state1 = state0.processPartition(partitionB)
      state1 should be(EventLogMembershipState(Set("A", "B"), Set("C"), Set()))
      val state2 = state1.processPartition(partitionC)
      state2 should be(EventLogMembershipState(Set("A", "B", "C"), Set("D"), Set()))
      val state3 = state2.processPartition(partitionD)
      state3 should be(EventLogMembershipState(Set("A", "B", "C", "D"), Set(), Set()))
    }
    "solve membership if receives an unexpected partition" in {
      val state0 = EventLogMembershipState(partitionA)
      state0 should be(EventLogMembershipState(Set("A"), Set("B", "C"), Set()))
      val state1 = state0.processPartition(partitionB)
      state1 should be(EventLogMembershipState(Set("A", "B"), Set("C"), Set()))
      val state2 = state1.processPartition(partitionD)
      state2 should be(EventLogMembershipState(Set("A", "B"), Set("C"), Set(UnconnectedPartition("D", Set("C")))))
      val state3 = state2.processPartition(partitionC)
      state3 should be(EventLogMembershipState(Set("A", "B", "C", "D"), Set(), Set()))
    }
    "solve membership if receives two unexpected partitions" in {
      val state0 = EventLogMembershipState(partitionB)
      state0 should be(EventLogMembershipState(Set("B"), Set("A"), Set()))
      val state1 = state0.processPartition(partitionD)
      state1 should be(EventLogMembershipState(Set("B"), Set("A"), Set(UnconnectedPartition("D", Set("C")))))
      val state2 = state1.processPartition(partitionC)
      state2 should be(EventLogMembershipState(Set("B"), Set("A"), Set(UnconnectedPartition("D", Set("C")), UnconnectedPartition("C", Set("A", "D")))))
      val state3 = state2.processPartition(partitionA)
      state3 should be(EventLogMembershipState(Set("A", "B", "C", "D"), Set(), Set()))
    }
  }
}
