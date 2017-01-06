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

import akka.actor.{ ActorRef, Props }
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.typesafe.config.{ Config, ConfigFactory }

import scala.util.{ Failure, Success }

object ReplicatedPersistOnEventSpec {

  case class PersistValue(value: Int)

  case class ValuePersisted(value: Int)

  case class ValuePersistedOnEvent(value: Int, processId: String)

  class PersistOnEventActor(val actorId: String, replicaId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with PersistOnEvent {

    override val id = s"s-${actorId}-${replicaId}"

    override val aggregateId = Some(actorId)

    override def onCommand: Receive = {
      case PersistValue(value) => persist(ValuePersisted(value))(Handler.empty)
    }

    override def onEvent: Receive = {
      case v: ValuePersistedOnEvent => probe ! v
      case ValuePersisted(value)    => persistOnEvent(ValuePersistedOnEvent(value * 10, replicaId))
    }
  }

  class PersistOnEventSmartActor(val actorId: String, replicaId: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor with PersistOnEvent {

    override val id = s"s-${actorId}-${replicaId}"

    override val aggregateId = Some(actorId)

    override def onCommand: Receive = {
      case PersistValue(value) => persist(ValuePersisted(value))(Handler.empty)
    }

    override def onEvent: Receive = {
      case v: ValuePersistedOnEvent => probe ! v
      case ValuePersisted(value) if (lastHandledEvent.localLogId == lastHandledEvent.processId) =>
        persistOnEvent(ValuePersistedOnEvent(value * 10, replicaId))

    }
  }

}

class ReplicatedPersistOnEventConfig(providerConfig: Config) extends MultiNodeReplicationConfig {
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

abstract class ReplicatedPersistOnEventSpec(config: ReplicatedPersistOnEventConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import ReplicatedPersistOnEventSpec._
  import config._
  import scala.concurrent.duration._

  def initialParticipants: Int =
    roles.size

  muteDeadLetters(classOf[AnyRef])(system)

  "Replicated actor" must {
    "PersistOnEvent in both sides" in {
      val probe = TestProbe()
      val logAName = "nodeA_ReplicatedPersistOnEventSpecLeveldb"
      val logBName = "nodeB_ReplicatedPersistOnEventSpecLeveldb"

      runOn(nodeA) {

        val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
        val actor = system.actorOf(Props(new PersistOnEventActor("actor1", "A", endpoint.log, probe.ref)))
        val smartActor = system.actorOf(Props(new PersistOnEventSmartActor("smartActor1", "A", endpoint.log, probe.ref)))

        enterBarrier("both-up")

        actor ! PersistValue(1)
        probe.expectMsg(ValuePersistedOnEvent(10, "A"))

        enterBarrier("waitingA")
        enterBarrier("waitingB")
        probe.expectMsg(ValuePersistedOnEvent(10, "B"))
        enterBarrier("waitingA2")
        probe.expectNoMsg(20.seconds)

        smartActor ! PersistValue(1)
        probe.expectMsg(ValuePersistedOnEvent(10, "A"))
        probe.expectNoMsg()
      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
        val actor = system.actorOf(Props(new PersistOnEventActor("actor1", "B", endpoint.log, probe.ref)))
        val smartActor = system.actorOf(Props(new PersistOnEventSmartActor("smartActor1", "B", endpoint.log, probe.ref)))

        enterBarrier("both-up")
        enterBarrier("waitingA")

        probe.expectMsgAllOf(ValuePersistedOnEvent(10, "A"), ValuePersistedOnEvent(10, "B"))

        enterBarrier("waitingB")
        enterBarrier("waitingA2")
        probe.expectNoMsg(20.seconds)

        probe.expectMsg(ValuePersistedOnEvent(10, "A"))
        probe.expectNoMsg()
      }

      enterBarrier("finish")

    }
  }

}
