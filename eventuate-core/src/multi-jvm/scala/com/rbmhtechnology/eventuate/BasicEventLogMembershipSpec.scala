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
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.log.EventLogMembershipProtocol.EventLogMembership
import com.rbmhtechnology.eventuate.log.EventLogMembershipProtocol.membershipAggregateId
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

class BasicEventLogMembershipSpecConfig(providerConfig: Config) extends EventuateMultiNodeSpecConfig with MultiNodeReplicationConfig {

  def endpointNodeWithConnections(name: String, logs: Set[String], connections: Set[String]): EventuateNodeTest = {
    super.endpointNodeWithLogs(name, logs, connections, customConfig(connections.size))
  }
  val log1 = "log1"
  val log2 = "log2"

  /**
   *      A - C
   *     /     \
   *    B       D
   */
  val nodeA = endpointNodeWithConnections("nodeA", Set(log1, log2), Set("nodeB", "nodeC"))
  val nodeB = endpointNodeWithConnections("nodeB", Set(log1, log2), Set("nodeA"))
  val nodeC = endpointNodeWithConnections("nodeC", Set(log1), Set("nodeA", "nodeD"))
  val nodeD = endpointNodeWithConnections("nodeD", Set(log1), Set("nodeC"))

  def partitions(logName: String): Set[String] = nodes.flatMap(_.partitionName(logName))

  // TODO
  def customConfig(connections: Int) = Some(ConfigFactory.parseString(s"eventuate.endpoint.connections.size = $connections"))

  val nodes = Set(nodeA, nodeB, nodeC, nodeD)
  val log1Partitions: Set[String] = partitions(log1)
  val log2Partitions: Set[String] = partitions(log2)

  testTransport(on = true)
  setConfig(providerConfig)
  setNodesConfig(nodes)

}

abstract class BasicEventLogMembershipSpec(config: BasicEventLogMembershipSpecConfig) extends EventuateMultiNodeSpec(config) {

  import Implicits._
  import config._

  override def initialParticipants: Int = roles.size

  override def logName: String = throw new UnsupportedOperationException

  class MembershipAwareActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedView {

    override def onCommand: Receive = Actor.emptyBehavior

    override def onEvent: Receive = {
      case EventLogMembership(members) => probe ! members
    }

  }

  val initialize = (e: ReplicationEndpoint) => {
    val memberships = e.logs.mapValues { logRef =>
      val probe = TestProbe()
      system.actorOf(Props(new MembershipAwareActor(s"membershipAware-${e.id}", logRef, probe.ref)))
      probe
    }

    memberships
  }

  "Event log membership" must {

    "reach agreement" in {
      nodeA.runWith(initialize, false) { (endpoint, memberships) =>
        testConductor.blackhole(nodeA, nodeC, Direction.Both).await
        testConductor.blackhole(nodeC, nodeD, Direction.Both).await
        enterBarrier("startup")
        endpoint.activate()
        val membershipLog1 = memberships(log1)
        val membershipLog2 = memberships(log2)

        membershipLog1.expectNoMsg()
        membershipLog2.expectNoMsg()

        testConductor.passThrough(nodeA, nodeC, Direction.Both).await
        enterBarrier("healedAC")

        membershipLog2.expectMsg(log2Partitions)
        membershipLog1.expectNoMsg()

        testConductor.passThrough(nodeC, nodeD, Direction.Both).await
        enterBarrier("healedCD")
        membershipLog1.expectMsg(log1Partitions)
      }

      nodeB.runWith(initialize, false) { (endpoint, memberships) =>
        enterBarrier("startup")
        endpoint.activate()
        val membershipLog1 = memberships(log1)
        val membershipLog2 = memberships(log2)

        membershipLog1.expectNoMsg()
        membershipLog2.expectNoMsg()
        enterBarrier("healedAC")

        membershipLog2.expectMsg(log2Partitions)
        membershipLog1.expectNoMsg()

        enterBarrier("healedCD")
        membershipLog1.expectMsg(log1Partitions)
      }

      nodeC.runWith(initialize, false) { (endpoint, memberships) =>
        enterBarrier("startup")
        endpoint.activate()
        val membershipLog1 = memberships(log1)

        membershipLog1.expectNoMsg()
        enterBarrier("healedAC")

        membershipLog1.expectNoMsg()
        enterBarrier("healedCD")

        membershipLog1.expectMsg(log1Partitions)
      }

      nodeD.runWith(initialize, false) { (endpoint, memberships) =>
        enterBarrier("startup")
        endpoint.activate()
        val membershipLog1 = memberships(log1)

        membershipLog1.expectNoMsg()
        enterBarrier("healedAC")

        membershipLog1.expectNoMsg()
        enterBarrier("healedCD")

        membershipLog1.expectMsg(log1Partitions)
      }

    }
  }

}
