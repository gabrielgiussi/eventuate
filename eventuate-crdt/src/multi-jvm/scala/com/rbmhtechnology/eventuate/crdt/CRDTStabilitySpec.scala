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

import akka.remote.transport.ThrottlerTransportAdapter.Direction._
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventuateMultiNodeSpec
import com.rbmhtechnology.eventuate.EventuateMultiNodeSpecConfig
import com.rbmhtechnology.eventuate.EventuateNodeTest
import com.rbmhtechnology.eventuate.MultiNodeConfigLeveldb
import com.rbmhtechnology.eventuate.MultiNodeReplicationConfig
import com.rbmhtechnology.eventuate.MultiNodeSupportLeveldb
import com.rbmhtechnology.eventuate.ReplicationEndpoint
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.AWSetService.AWSet
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.log.StabilityProtocol
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config

class CRDTStabilitySpecLeveldb extends CRDTStabilitySpec(new CRDTStabilitySpecConfig(MultiNodeConfigLeveldb.providerConfig)) with MultiNodeSupportLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode1 extends CRDTStabilitySpecLeveldb
class CRDTStabilitySpecLeveldbMultiJvmNode2 extends CRDTStabilitySpecLeveldb
//class CRDTStabilitySpecLeveldbMultiJvmNode3 extends CRDTStabilitySpecLeveldb

class CRDTStabilitySpecConfig(providerConfig: Config) extends EventuateMultiNodeSpecConfig with MultiNodeReplicationConfig {

  val logName = "stabilityLog"

  val nodeA = endpointNode("nodeA", logName, Set("nodeB"))
  val nodeB = endpointNode("nodeB", logName, Set("nodeA"))
  //val nodeC = endpointNode("nodeC", logName, Set("nodeA","nodeD"))
  //val nodeD = endpointNode("nodeD", logName, Set("nodeC"))

  testTransport(on = true)

  setConfig(providerConfig)

}

abstract class CRDTStabilitySpec(config: CRDTStabilitySpecConfig) extends EventuateMultiNodeSpec(config) {

  import Implicits._
  import config._

  override val logName = config.logName

  def initialParticipants: Int = roles.size

  case class StableCRDT[A](value: A, pologSize: Int)

  def stable(entries: (EventuateNodeTest, Long)*) = TCStable(VectorTime(entries.map { case (e, l) => (e.partitionName(config.logName).get, l) }: _*))

  val awset1 = "awset1"

  val initialize = (expected: TCStable) => (e: ReplicationEndpoint) => {
    val stableProbe = TestProbe()
    val changeProbe = TestProbe()
    val service = new AWSetService[Int](e.id, e.log) {
      override private[crdt] def onStable(crdt: AWSet[Int], stable: StabilityProtocol.TCStable): Unit = {
        if (stable equiv expected)
          stableProbe.ref ! StableCRDT(ops.value(crdt), crdt.polog.log.size)
      }

      override private[crdt] def onChange(crdt: AWSet[Int], operation: Option[Operation], vt: Option[VectorTime]): Unit = {
        changeProbe.ref ! operation
      }
    }
    (stableProbe, changeProbe, service)
  }

  "Pure op based CRDT" must {
    "receive TCStable" in {

      nodeA.runWith(initialize(stable(nodeB -> 1))) {
        case (_, (stableProbe, changeProbe, service)) =>
          service.activateStability(nodeA.partitionName(logName).get, Set(nodeB.partitionName(config.logName).get)) // Document & enforce this!
          testConductor.blackhole(nodeA, nodeB, Both).await
          enterBarrier("broken")

          service.add(awset1, 1)
          service.add(awset1, 2)

          //changeProbe.expectMsg(AddOp(1))
          //changeProbe.expectMsg(AddOp(2))

          enterBarrier("repairAB")
          testConductor.passThrough(nodeA, nodeB, Both).await
          //changeProbe.expectMsg(AddOp(3))

          stableProbe.expectMsg(StableCRDT(Set(1, 2, 3), 2))
      }

      nodeB.runWith(initialize(stable(nodeA -> 2))) {
        case (_, (stableProbe, changeProbe, service)) =>
          service.activateStability(nodeB.partitionName(logName).get, Set(nodeA.partitionName(config.logName).get))
          enterBarrier("broken")

          service.add(awset1, 3)
          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          stableProbe.expectMsg(StableCRDT(Set(1, 2, 3), 1))
      }

    }
  }

}
