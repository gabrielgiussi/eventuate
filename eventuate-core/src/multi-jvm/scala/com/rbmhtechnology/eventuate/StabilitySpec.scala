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
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.rbmhtechnology.eventuate.log.StabilityChannel.SubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.annotation.tailrec
import scala.util.Try

class StabilitySpecConfig(providerConfig: Config) extends EventuateMultiNodeSpecConfig with MultiNodeReplicationConfig {

  val logName = "stabilityLog"

  val nodeA = endpointTest("nodeA", Set("nodeB", "nodeC"))
  val nodeB = endpointTest("nodeB", Set("nodeA"))
  val nodeC = endpointTest("nodeC", Set("nodeA"))

  testTransport(on = true) // TODO review

  val customConfig = ConfigFactory.parseString(
    s"""
       |akka.loglevel = OFF
       |eventuate.log.write-batch-size = 1
       |eventuate.log.stability.partitions = [${Set(nodeA, nodeB, nodeC).map(_.partitionName).mkString(",")}]
    """.stripMargin)

  setConfig(customConfig.withFallback(providerConfig))
}

object StabilitySpec {

  case class Save(payload: String)

  case class SavedValue(payload: String)

  class DummyActor(val id: String, val eventLog: ActorRef) extends EventsourcedActor {

    override def onCommand: Receive = {
      case Save(p) => persist(SavedValue(p))(Handler.empty)
    }

    override def onEvent: Receive = Actor.emptyBehavior
  }

  class TCStableActor(tcs: TCStable, probe: ActorRef) extends Actor with ActorLogging {

    override def receive: Receive = {
      case t: TCStable if t equiv tcs => probe ! "stable"
      case t: TCStable                => log.error("Stable => {}", t)
    }
  }

}

abstract class StabilitySpec(config: StabilitySpecConfig) extends EventuateMultiNodeSpec(config) {

  import Implicits._
  import StabilitySpec._
  import config._

  import scala.concurrent.duration._

  def initialParticipants: Int =
    roles.size

  implicit class EnhancedTestProbe(probe: TestProbe) {
    def expectMsgWithin[T](max: FiniteDuration, obj: T): Unit = {
      @tailrec
      def _expect(): Unit = try {
        expectMsg(obj)
      } catch {
        case e: AssertionError =>
          if (!e.getMessage.contains("timeout")) _expect()
          else throw e
      }

      within(max) {
        _expect()
      }
    }
  }

  val expected = TCStable(VectorTime("nodeA_stabilityLog" -> 4, "nodeB_stabilityLog" -> 3, "nodeC_stabilityLog" -> 2))

  val initialize = (e: ReplicationEndpoint) => {
    val stableProbe = TestProbe()
    val subscribe = system.actorOf(Props(new TCStableActor(expected, stableProbe.ref)), "stableActor")
    val actor = system.actorOf(Props(new DummyActor("dummy", e.log)), "dummy")

    e.log ! SubscribeTCStable(subscribe)

    (stableProbe, actor)
  }

  "eventlog" must {
    "" in {

      nodeA.runWith(initialize) {
        case (_, (stableProbe, actor)) =>
          actor ! Save("a1")
          actor ! Save("a2")
          actor ! Save("a3")
          actor ! Save("a4")
          stableProbe.expectMsg("stable")
      }

      nodeB.runWith(initialize) {
        case (_, (stableProbe, actor)) =>
          actor ! Save("b1")
          actor ! Save("b2")
          actor ! Save("b3")
          stableProbe.expectMsg("stable")
          testConductor.passThrough()
      }

      nodeC.runWith(initialize) {
        case (_, (stableProbe, actor)) =>
          actor ! Save("c1")
          actor ! Save("c2")
          stableProbe.expectMsg("stable")
      }
    }
  }
}
