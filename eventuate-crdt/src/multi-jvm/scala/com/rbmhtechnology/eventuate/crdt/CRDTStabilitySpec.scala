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

import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventuateMultiNodeSpec
import com.rbmhtechnology.eventuate.MultiNodeConfigLeveldb
import com.rbmhtechnology.eventuate.MultiNodeSupportLeveldb
import com.rbmhtechnology.eventuate.ReplicationEndpoint
import com.rbmhtechnology.eventuate.StabilitySpecConfig
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.AWSetService.AWSet
import com.rbmhtechnology.eventuate.log.StabilityProtocol
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable

class CRDTStabilitySpecLeveldb extends CRDTStabilitySpec(new StabilitySpecConfig(MultiNodeConfigLeveldb.providerConfig)) with MultiNodeSupportLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode1 extends CRDTStabilitySpecLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode2 extends CRDTStabilitySpecLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode3 extends CRDTStabilitySpecLeveldb

abstract class CRDTStabilitySpec(config: StabilitySpecConfig) extends EventuateMultiNodeSpec(config) {

  import Implicits._
  import config._

  def initialParticipants: Int = roles.size

  case class StableCRDT[A](value: A, pologSize: Int)

  val expected = TCStable(VectorTime(nodeA.partitionName -> 2, nodeB.partitionName -> 1, nodeC.partitionName -> 0))

  val initialize = (e: ReplicationEndpoint) => {
    val stableProbe = TestProbe()
    val service = new AWSetService[Int]("awset-test-service", e.log) {
      override private[crdt] def onStable(crdt: AWSet[Int], stable: StabilityProtocol.TCStable): Unit = {
        if (stable equiv expected)
          stableProbe.ref ! StableCRDT(ops.value(crdt), crdt.polog.log.size)
      }
    }
    (stableProbe, service)
  }

  "Pure op based CRDT" must {
    "receive TCStable" in {

      nodeA.runWith(initialize) {
        case (_, (stableProbe, service)) =>
          testConductor.blackhole(nodeA, nodeB, Direction.Both).await
          testConductor.blackhole(nodeA, nodeC, Direction.Both).await
          enterBarrier("broken")

          service.add("awset1", 1)
          service.add("awset1", 2)
          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          testConductor.passThrough(nodeA, nodeB, Direction.Both).await
          stableProbe.expectNoMsg()

          enterBarrier("repairAC")
          testConductor.passThrough(nodeA, nodeC, Direction.Both).await
          stableProbe.expectMsg(StableCRDT(Set(1, 2, 3), 0))
      }

      nodeB.runWith(initialize) {
        case (_, (stableProbe, service)) =>
          enterBarrier("broken")

          service.add("awset1", 3)
          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          stableProbe.expectNoMsg()

          enterBarrier("repairAC")
          stableProbe.expectMsg(StableCRDT(Set(1, 2, 3), 0))
      }

      nodeC.runWith(initialize) {
        case (_, (stableProbe, service)) =>
          enterBarrier("broken")

          service.value("awset1")
          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          stableProbe.expectNoMsg()

          enterBarrier("repairAC")
          stableProbe.expectMsg(StableCRDT(Set(1, 2, 3), 0))
      }
    }
  }

}
