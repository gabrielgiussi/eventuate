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

import akka.actor.{ ActorRef, ActorSystem }
import akka.remote.testkit.MultiNodeSpec
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.{ ReplicationConnection, _ }
import com.typesafe.config.ConfigFactory

import scala.collection.immutable.Set

class ReplicatedCERMatchSpec2Leveldb extends ReplicatedCERMatchSpec2 with MultiNodeSupportLeveldb

class ReplicatedCERMatchSpec2LeveldbMultiJvmNode1 extends ReplicatedCERMatchSpec2Leveldb

class ReplicatedCERMatchSpec2LeveldbMultiJvmNode2 extends ReplicatedCERMatchSpec2Leveldb

class ReplicatedCERMatchSpec2LeveldbMultiJvmNode3 extends ReplicatedCERMatchSpec2Leveldb

object ReplicatedCERMatchSpec2 {

  class TestMatchService(val probe: TestProbe, override val serviceId: String, override val log: ActorRef)(implicit val s: ActorSystem, val o: CRDTServiceOps[CERMatch, Set[String]]) extends MatchService(serviceId, log)(s, o) {
    override private[crdt] def onChange(crdt: CERMatch, operation: Any): Unit = probe.ref ! crdt.value

    override private[crdt] def onApology(crdt: CERMatch, apology: Apology, processId: String): Unit = probe.ref ! (apology.undo.value, processId)
  }

}

object ReplicatedCERMatchConfig2 extends MultiNodeReplicationConfig {
  val coordinator = role("coordinator")
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

abstract class ReplicatedCERMatchSpec2 extends MultiNodeSpec(ReplicatedCERMatchConfig2) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {

  import ReplicatedCERMatchConfig2._
  import ReplicatedCERMatchSpec2._

  def initialParticipants: Int = roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  @volatile var serviceA: MatchService = _
  @volatile var serviceB: MatchService = _
  @volatile var firstSystem: ActorSystem = _
  @volatile var firstSystemRestarted: ActorSystem = _
  @volatile var secondSystem: ActorSystem = _
  @volatile var secondSystemRestarted: ActorSystem = _

  override def afterAll(): Unit = {
    if (secondSystem != null)
      shutdown(secondSystem)
    if (firstSystem != null)
      shutdown(firstSystem)
    super.afterAll()
  }

  "A replicated CERMatch" must {
    "converge" in {
      val probe = TestProbe()

      runOn(coordinator) {
        // startNewSystem() COMO FUNCIONA ESTO? Puedo hacer un shutdown y dsp un startNewSystem?
        enterBarrier("bothUp")
        enterBarrier("bothSync")
        enterBarrier("nodeBDown")
        enterBarrier("bothDown")
        enterBarrier("waitingA")
        enterBarrier("bothUpAgain")
      }

      runOn(nodeA) {
        firstSystem = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4992").
            withFallback(system.settings.config))
        val nodeBAddress = node(nodeB).address
        val endpoint = createEndpoint(nodeA.name, Set(ReplicationConnection(nodeBAddress.host.get, 4993, nodeBAddress.system)))(firstSystem)
        serviceA = new TestMatchService(probe, "A", endpoint.log)
        /*
        serviceA = new MatchService("A", endpoint.log) {
          override private[crdt] def onChange(crdt: CERMatch, operation: Any): Unit = probe.ref ! crdt.value

          override private[crdt] def onApology(crdt: CERMatch, apology: Apology): Unit = probe.ref ! apology.undo.value
        }*/
        enterBarrier("bothUp")
      }

      runOn(nodeB) {
        secondSystem = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4993").
            withFallback(system.settings.config))
        val nodeAAddress = node(nodeA).address
        val endpoint = createEndpoint(nodeB.name, Set(ReplicationConnection(nodeAAddress.host.get, 4992, nodeAAddress.system)))(secondSystem)
        /*
        serviceB = new MatchService("B", endpoint.log) {
          override private[crdt] def onChange(crdt: CERMatch, operation: Any): Unit = probe.ref ! crdt.value

          override private[crdt] def onApology(crdt: CERMatch, apology: Apology): Unit = probe.ref ! apology.undo.value
        }*/
        serviceB = new TestMatchService(probe, "B", endpoint.log)
        enterBarrier("bothUp")
      }

      runOn(nodeA) {
        serviceA.add("match1", "Gallardo")
        probe.expectMsg(Set("Gallardo"))
        probe.expectMsg(Set("Gallardo", "Funes Mori"))
        enterBarrier("bothSync")
        enterBarrier("nodeBDown")
        serviceA.add("match1", "Astrada")
        probe.expectMsg(Set("Gallardo", "Funes Mori", "Astrada"))
        shutdown(firstSystem)
        enterBarrier("bothDown")
        // B adds Ortega
        enterBarrier("waitingA")
        firstSystemRestarted = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4992").
            withFallback(system.settings.config))
        val nodeBAddress = node(nodeB).address
        val endpoint = createEndpoint(nodeA.name, Set(ReplicationConnection(nodeBAddress.host.get, 4993, nodeBAddress.system)))(firstSystemRestarted)
        serviceA = new TestMatchService(probe, "A-Restarted", endpoint.log)
        enterBarrier("bothUpAgain")
        serviceA.value("match1")
        probe.expectMsg(Set("Gallardo"))
        probe.expectMsg(Set("Gallardo", "Funes Mori"))
        probe.expectMsg(Set("Gallardo", "Funes Mori", "Astrada"))
        probe.expectMsg(Set("Gallardo", "Funes Mori", "Astrada")) // Este mensaje lo recibe por que pido serviceA
        probe.expectMsg(("Ortega", "nodeB_ReplicatedCERMatchSpec2Leveldb"))
        probe.expectNoMsg()
      }

      runOn(nodeB) {
        serviceB.value("match1")
        probe.expectMsg(Set("Gallardo"))
        serviceB.add("match1", "Funes Mori")
        probe.expectMsg(Set("Gallardo", "Funes Mori"))
        enterBarrier("bothSync")
        shutdown(secondSystem)
        enterBarrier("nodeBDown")
        // A adds Astrada
        enterBarrier("bothDown")
        secondSystemRestarted = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4993").
            withFallback(system.settings.config))
        val nodeAAddress = node(nodeA).address
        val endpoint = createEndpoint(nodeB.name, Set(ReplicationConnection(nodeAAddress.host.get, 4992, nodeAAddress.system)))(secondSystemRestarted)
        serviceB = new TestMatchService(probe, "B-Restarted", endpoint.log)
        // Concurrent to Astrada
        serviceB.add("match1", "Ortega")
        probe.expectMsg(Set("Gallardo"))
        probe.expectMsg(Set("Gallardo", "Funes Mori"))
        probe.expectMsg(Set("Gallardo", "Funes Mori", "Ortega"))
        enterBarrier("waitingA")
        enterBarrier("bothUpAgain")
        probe.expectMsg(Set("Gallardo", "Funes Mori", "Astrada"))
        probe.expectMsg(("Ortega", "nodeB_ReplicatedCERMatchSpec2Leveldb"))
        probe.expectNoMsg()
      }

      /*   runOn(nodeA) {
        serviceA.add("match1", "Astrada")
        probe.expectMsg(Set("Gallardo", "Funes Mori", "Astrada"))
        shutdown(system)
      }

      runOn(nodeB) {
        // startNewSystem() NO FUNCIONA
        val secondSystem = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4993").
            withFallback(system.settings.config))
        enterBarrier("nodeADown")
        val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))(secondSystem)
        probe.expectNoMsg()

      }*/

      enterBarrier("finish")
    }
  }

}
