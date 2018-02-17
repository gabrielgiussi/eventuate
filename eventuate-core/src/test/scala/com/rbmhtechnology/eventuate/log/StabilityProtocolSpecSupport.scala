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

package com.rbmhtechnology.eventuate.log

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.log.EventLogMembershipProtocol.EventLogMembership
import com.rbmhtechnology.eventuate.log.StabilityProtocol.StableVT
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable

object StabilityProtocolSpecSupport {

  trait ClusterAB {
    def stabilityChecker()(implicit system: ActorSystem) = system.actorOf(Props[StabilityChecker])

    def A = "A"
    def B = "B"
    def partitions = Set(A, B)
    def vt(a: Long, b: Long) = VectorTime(A -> a, B -> b)
    def tcstable(a: Long, b: Long) = TCStable(vt(a, b))
    def initialRTM = partitions.map(_ -> VectorTime.Zero).toMap

    def askTCStable(st: ActorRef)(implicit sender: ActorRef) = {
      st ! EventLogMembership(partitions)
      st ! StableVT
    }
  }

  trait ClusterABC extends ClusterAB {
    def C = "C"
    override def partitions: Set[String] = super.partitions + C
    def vt(a: Long, b: Long, c: Long) = VectorTime(A -> a, B -> b, C -> c)
    def tcstable(a: Long, b: Long, c: Long) = TCStable(vt(a, b, c))
  }

}
