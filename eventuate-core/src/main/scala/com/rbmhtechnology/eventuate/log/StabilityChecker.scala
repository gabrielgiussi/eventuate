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

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Props
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.log.StabilityProtocol.MostRecentlyViewedTimestamps
import com.rbmhtechnology.eventuate.log.StabilityProtocol.RTM
import com.rbmhtechnology.eventuate.log.StabilityProtocol.StableVT

object StabilityChecker {

  def props(partitions: Set[String]) = Props(new StabilityChecker(partitions)) // TODO add partitions
}

// TODO remove default empty set
class StabilityChecker(partitions: Set[String] = Set.empty) extends Actor with ActorLogging {

  var recentTimestampsMatrix: RTM = partitions.map(_ -> VectorTime.Zero).toMap

  /*
  def receiveUpdates: Receive = {
    case MostRecentlyViewedTimestamps(timestamps) =>
      recentTimestampsMatrix = StabilityProtocol.updateRTM(recentTimestampsMatrix)(timestamps)
  }

  def emitting: Receive = {
    case StableVT =>
      sender() ! StabilityProtocol.stableVectorTime(recentTimestampsMatrix.values.toSeq)
  }

  override def receive = receiveUpdates orElse {
    case EventLogMembership(members) =>
      recentTimestampsMatrix = StabilityProtocol.updateRTM(recentTimestampsMatrix)(members.map(_ -> VectorTime.Zero).toMap)
      context.become(receiveUpdates orElse emitting)
  }
  */

  override def receive: Receive = {
    case StableVT =>
      sender() ! StabilityProtocol.stableVectorTime(recentTimestampsMatrix.values.toSeq)
    case MostRecentlyViewedTimestamps(timestamps) =>
      recentTimestampsMatrix = StabilityProtocol.updateRTM(recentTimestampsMatrix)(timestamps)
  }

}
