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

  def props(partitions: Set[String]) = Props(new StabilityChecker(partitions))
}

// TODO i have 2 options,
// (1) Receive the partitions on construction (in this case the actor should be created only after been seing the
//     entire neighboorhood (it could not receive MostRecentlyUpdated meanwhile but this should be donde quickly when the cluster starts
// (2) Receive an Enable after seing the neighboorhod
class StabilityChecker(partitions: Set[String]) extends Actor with ActorLogging {

  // Remember that the the list of partitions must match what each eventlog puts in its event log
  // Example, partitions Set[A_log1,B_log2] if the Vector is builde A_log1 -> 0, B_log1 -> 0
  var recentTimestampsMatrix: RTM = partitions.map(_ -> VectorTime.Zero).toMap

  override def receive = {
    case MostRecentlyViewedTimestamps(timestamps) =>
      log.debug(s"Received $timestamps, current RTM: $recentTimestampsMatrix")
      recentTimestampsMatrix = StabilityProtocol.updateRTM(recentTimestampsMatrix)(timestamps)
      log.debug(s"Updated RTM: $recentTimestampsMatrix")

    case StableVT =>
      sender() ! StabilityProtocol.stableVectorTime(recentTimestampsMatrix.values.toSeq)

  }

}
//     A
//   /  \
// C     B -- D
// B should annotate when he sees a VectorClock that strictly contains an event from D
// Per event log
class Neighborhood() {

}