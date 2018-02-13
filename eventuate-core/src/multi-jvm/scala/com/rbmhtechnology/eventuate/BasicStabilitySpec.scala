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
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.rbmhtechnology.eventuate.log.StabilityChannel.SubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

class StabilitySpecConfig(providerConfig: Config) extends EventuateMultiNodeSpecConfig with MultiNodeReplicationConfig {

  val logName = "stabilityLog"

  val nodeA = endpointTest("nodeA", Set("nodeB", "nodeC"))
  val nodeB = endpointTest("nodeB", Set("nodeA"))
  val nodeC = endpointTest("nodeC", Set("nodeA"))

  testTransport(on = true)

  val customConfig = ConfigFactory.parseString(
    s"""
       |akka.loglevel = ERROR
       |eventuate.log.stability.partitions = [${Set(nodeA, nodeB, nodeC).map(_.partitionName).mkString(",")}]
    """.stripMargin)

  setConfig(customConfig.withFallback(providerConfig))
}

object BasicStabilitySpec {

  case class Save(payload: String)

  case class SavedValue(payload: String)

  class DummyActor(val id: String, val eventLog: ActorRef) extends EventsourcedActor {

    override def onCommand: Receive = {
      case Save(p) => persist(SavedValue(p))(Handler.empty)
    }

    override def onEvent: Receive = Actor.emptyBehavior
  }

  case class Expect(tcs: TCStable)

  class TCStableActor(probe: ActorRef) extends Actor with ActorLogging {

    override def receive: Receive = {
      case t: TCStable => probe ! t
      case Expect(t)   => context.become(expecting(t))
    }

    def expecting(tcs: TCStable): Receive = {
      case t: TCStable if t equiv tcs => probe ! "stable"
      case t: TCStable                => ()
    }
  }

}

abstract class BasicStabilitySpec(config: StabilitySpecConfig) extends EventuateMultiNodeSpec(config) {

  import Implicits._
  import BasicStabilitySpec._
  import config._

  def initialParticipants: Int =
    roles.size

  val expected = TCStable(VectorTime(nodeA.partitionName -> 4, nodeB.partitionName -> 3, nodeC.partitionName -> 0))

  val initialize = (e: ReplicationEndpoint) => {
    val stableProbe = TestProbe()
    val subscribed = system.actorOf(Props(new TCStableActor(stableProbe.ref)), "stableActor")
    val actor = system.actorOf(Props(new DummyActor("dummy", e.log)), "dummy")

    e.log ! SubscribeTCStable(subscribed)

    (stableProbe, subscribed, actor)
  }

  "Replicated EventLogs across different endpoints" must {
    "emit TCStable" in {

      nodeA.runWith(initialize) {
        case (_, (stableProbe, subscribed, actor)) =>
          testConductor.blackhole(nodeA, nodeB, Direction.Both).await
          testConductor.blackhole(nodeA, nodeC, Direction.Both).await
          enterBarrier("broken")

          actor ! Save("a1")
          actor ! Save("a2")
          actor ! Save("a3")
          actor ! Save("a4")
          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          testConductor.passThrough(nodeA, nodeB, Direction.Both).await
          stableProbe.expectNoMsg()

          subscribed ! Expect(expected)
          enterBarrier("repairAC")
          testConductor.passThrough(nodeA, nodeC, Direction.Both).await
          stableProbe.expectMsg("stable")
      }

      nodeB.runWith(initialize) {
        case (_, (stableProbe, subscribed, actor)) =>
          enterBarrier("broken")

          actor ! Save("b1")
          actor ! Save("b2")
          actor ! Save("b3")
          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          stableProbe.expectNoMsg()

          subscribed ! Expect(expected)
          enterBarrier("repairAC")
          stableProbe.expectMsg("stable")
      }

      nodeC.runWith(initialize) {
        case (_, (stableProbe, subscribed, actor)) =>
          enterBarrier("broken")

          stableProbe.expectNoMsg()

          enterBarrier("repairAB")
          stableProbe.expectNoMsg()

          subscribed ! Expect(expected)
          enterBarrier("repairAC")
          stableProbe.expectMsg("stable")
      }
    }
  }
}
