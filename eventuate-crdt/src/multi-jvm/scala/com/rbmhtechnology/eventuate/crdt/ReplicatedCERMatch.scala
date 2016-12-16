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

import akka.remote.testkit.MultiNodeSpec
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate._
import com.typesafe.config.ConfigFactory

class ReplicatedCERMatchSpecLeveldb extends ReplicatedCERMatchSpec with MultiNodeSupportLeveldb
class ReplicatedCERMatchMultiJvmNode1 extends ReplicatedCERMatchSpecLeveldb
class ReplicatedCERMatchMultiJvmNode2 extends ReplicatedCERMatchSpecLeveldb

object ReplicatedCERMatchConfig extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  val customConfig = ConfigFactory.parseString(
    """
      |eventuate.log.write-batch-size = 200
      |eventuate.log.replication.remote-read-timeout = 2s
    """.stripMargin)

  testTransport(on = true)

  setConfig(customConfig.withFallback(MultiNodeConfigLeveldb.providerConfig))
}

abstract class ReplicatedCERMatchSpec extends MultiNodeSpec(ReplicatedCERMatchConfig) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {

  import ReplicatedCERMatchConfig._

  def initialParticipants: Int = roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "A replicated CERMatch" must {
    "converge" in {
      val probe = TestProbe()

      runOn(nodeA) {
        val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
        val service = new MatchService("A", endpoint.log) {
          override private[crdt] def onChange(crdt: CERMatch, operation: Any): Unit = probe.ref ! crdt.value
        }

        service.add("match1", "Gallardo")
        probe.expectMsg(Set("Gallardo"))
        probe.expectMsg(Set("Gallardo", "Funes Mori"))

        // network partition
        testConductor.blackhole(nodeA, nodeB, Direction.Both).await
        enterBarrier("broken")

        // this is concurrent to service.remove("x", 1) on node B
        service.add("match1", "Astrada")
        probe.expectMsg(Set("Gallardo", "Funes Mori","Astrada"))

        enterBarrier("repair")
        testConductor.passThrough(nodeA, nodeB, Direction.Both).await

        probe.expectMsg(Set("Gallardo", "Ortega","Astrada"))
      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
        val service = new MatchService("B", endpoint.log) {
          override private[crdt] def onChange(crdt: CERMatch, operation: Any): Unit = probe.ref ! crdt.value
        }

        service.value("match1")
        probe.expectMsg(Set("Gallardo"))
        service.add("match1", "Funes Mori")
        probe.expectMsg(Set("Gallardo", "Funes Mori"))

        enterBarrier("broken")

        // this is concurrent to service.add("x", 1) on node A
        service.remove("match1", "Ortega")
        probe.expectMsg(Set("Gallardo", "Funes Mori","Ortega"))

        enterBarrier("repair")

        probe.expectMsg(Set("Gallardo", "Ortega","Astrada"))
      }

      enterBarrier("finish")
    }
  }

}
