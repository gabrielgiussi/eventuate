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

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.remote.testkit.MultiNodeSpec
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.rbmhtechnology.eventuate.log.StabilityProtocol.StableVT
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.util.Try

class StabilitySpecConfig(providerConfig: Config) extends MultiNodeReplicationConfig {

  val nodeA = role("nodeA")
  val nodeB = role("nodeB")
  //val nodeC = role("nodeC")

  val nodeAEventLog = s"${nodeA.name}_StabilitySpecLeveldb"
  val nodeBEventLog = s"${nodeB.name}_StabilitySpecLeveldb"

  testTransport(on = true) // TODO review

  val customConfig = ConfigFactory.parseString(
    s"""
       |eventuate.log.stability.partitions = [$nodeAEventLog,$nodeBEventLog]
    """.stripMargin) // TODO

  setConfig(customConfig.withFallback(providerConfig))
}

object StabilitySpec {

  val nodeALog = "nodeA_StabilitySpecLeveldb"
  val nodeBLog = "nodeB_StabilitySpecLeveldb"

  case class Save(payload: String)
  case class SavedValue(payload: String)

  implicit class EnhancedProbe(probe: TestProbe) {
    def expectAnytime[T](obj: T, limit: Int = 5): T = {
      Try { probe.expectMsg(obj) }.recover {
        case a: AssertionError if (limit > 1) =>
          println("Assertion error" + a.getMessage)
          expectAnytime(obj, limit - 1)
      }.get
    }
  }

  class DummyActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedActor {

    override def onCommand: Receive = {
      case Save(p) => persist(SavedValue(p))(Handler.empty)
    }

    override def onEvent: Receive = {
      case SavedValue("end") => probe ! s"continue"
    }
  }
}

abstract class StabilitySpec(config: StabilitySpecConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {
  import StabilitySpec._
  import config._

  def initialParticipants: Int =
    roles.size

  "eventlog" must {
    "" in {
      runOn(nodeA) {
        val endpoint = createEndpoint(nodeA.name, Set(node(nodeB).address.toReplicationConnection))
        val probe = TestProbe()
        val actor = system.actorOf(Props(new DummyActor("dummyA", endpoint.log, probe.ref)))

        endpoint.log.tell(StableVT, probe.ref)
        probe.expectMsg(TCStable(VectorTime(nodeALog -> 0, nodeBLog -> 0)))

        actor ! Save("a")
        actor ! Save("end")
        probe.expectMsg("continue")
        probe.expectMsg("continue")
        Thread.sleep(1000)
        endpoint.log.tell(StableVT, probe.ref)
        probe.expectMsg(TCStable(VectorTime(nodeALog -> 2, nodeBLog -> 2)))

      }

      runOn(nodeB) {
        val endpoint = createEndpoint(nodeB.name, Set(node(nodeA).address.toReplicationConnection))
        val probe = TestProbe()
        val actor = system.actorOf(Props(new DummyActor("dummyB", endpoint.log, probe.ref)))

        actor ! Save("b")
        actor ! Save("end")
        probe.expectMsg("continue")
        probe.expectMsg("continue")
        Thread.sleep(1000)
        probe.expectMsg(TCStable(VectorTime(nodeALog -> 2, nodeBLog -> 2)))
      }
    }
  }
}
