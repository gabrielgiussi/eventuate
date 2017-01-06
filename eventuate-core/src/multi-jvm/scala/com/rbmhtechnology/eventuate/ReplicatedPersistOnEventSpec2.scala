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

package com.rbmhtechnology.eventuate

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.typesafe.config.{ Config, ConfigFactory }

object ReplicatedPersistOnEventSpec2 {

  case class PersistValue(value: Int)

  case class ValuePersisted(value: Int)

  case class ValuePersistedOnEvent(value: Int, processId: String)

  class PersistOnEventSmartActor(val actorId: String, replicaId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with PersistOnEvent {

    override val id = s"s-${actorId}-${replicaId}"

    override val aggregateId = Some(actorId)

    override def onCommand: Receive = {
      case PersistValue(value) => persist(ValuePersisted(value))(Handler.empty)
    }

    override def onEvent: Receive = {
      case v: ValuePersistedOnEvent if (!recovering) => probe ! v
      case ValuePersisted(value) if (!recovering) && (lastHandledEvent.localLogId != lastHandledEvent.processId) =>
        persistOnEvent(ValuePersistedOnEvent(value * 10, replicaId))

    }
  }

}

class ReplicatedPersistOnEvent2Config(providerConfig: Config) extends MultiNodeReplicationConfig {
  val nodeA = role("nodeA")
  val nodeB = role("nodeB")

  setConfig(providerConfig)

  /*
  val customConfig = ConfigFactory.parseString(
    """
      |eventuate.log.write-batch-size = 200
      |eventuate.log.replication.remote-read-timeout = 2s
    """.stripMargin)

  testTransport(on = true)

  setConfig(customConfig.withFallback(MultiNodeConfigLeveldb.providerConfig))
  */
}

abstract class ReplicatedPersistOnEventSpec2(config: ReplicatedPersistOnEvent2Config) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {

  import ReplicatedPersistOnEventSpec2._
  import config._

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  @volatile var firstSystem: ActorSystem = _
  @volatile var secondSystem: ActorSystem = _
  @volatile var secondSystemRestarted: ActorSystem = _

  "Replicated EventsourcedActor" must {
    "PersistOnEvent after shutdown" in {
      val probe = TestProbe()
      runOn(nodeA) {
        firstSystem = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4992").
            withFallback(system.settings.config))
        val nodeBAddress = node(nodeB).address
        val endpoint = createEndpoint(nodeA.name, Set(ReplicationConnection(nodeBAddress.host.get, 4993, nodeBAddress.system)))(firstSystem)

        enterBarrier("bothUp")

        val actor = firstSystem.actorOf(Props(new PersistOnEventSmartActor("actor1", "A", endpoint.log, probe.ref)))
        actor ! PersistValue(1)
        probe.expectMsg(ValuePersistedOnEvent(10, "B"))
        probe.expectNoMsg()

        enterBarrier("nodeBDown")

        actor ! PersistValue(1)

        enterBarrier("bothUpAgain")

        probe.expectMsg(ValuePersistedOnEvent(10, "B"))
        probe.expectNoMsg()
      }

      runOn(nodeB) {
        secondSystem = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4993").
            withFallback(system.settings.config))
        val nodeAAddress = node(nodeA).address
        val endpoint = createEndpoint(nodeB.name, Set(ReplicationConnection(nodeAAddress.host.get, 4992, nodeAAddress.system)))(secondSystem)

        enterBarrier("bothUp")

        val actor = secondSystem.actorOf(Props(new PersistOnEventSmartActor("actor1", "B", endpoint.log, probe.ref)))
        probe.expectMsg(ValuePersistedOnEvent(10, "B"))
        probe.expectNoMsg()

        shutdown(secondSystem)
        enterBarrier("nodeBDown")

        secondSystemRestarted = ActorSystem(
          system.name,
          ConfigFactory.parseString("akka.remote.netty.tcp.port=4993").
            withFallback(system.settings.config))
        val endpoint2 = createEndpoint(nodeB.name, Set(ReplicationConnection(nodeAAddress.host.get, 4992, nodeAAddress.system)))(secondSystemRestarted)

        enterBarrier("bothUpAgain")

        val actorRestarted = secondSystemRestarted.actorOf(Props(new PersistOnEventSmartActor("actor1", "B", endpoint2.log, probe.ref)))
        probe.expectMsg(ValuePersistedOnEvent(10, "B"))
        probe.expectNoMsg()
      }

      enterBarrier("finish")
    }
  }
}
