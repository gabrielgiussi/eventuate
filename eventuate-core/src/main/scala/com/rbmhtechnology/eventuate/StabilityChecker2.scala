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
import akka.actor.Props

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object StabilityChecker2 {

  type StabilityCheck = Seq[VectorTime] => VectorTime => Boolean
  type StableEvents = Seq[VectorTime] => Set[VectorTime] => Set[VectorTime]
  type SafeStableEvents = Seq[VectorTime] => Set[VectorTime] => Try[Set[VectorTime]]

  case class WritedEvents(events: Set[VectorTime])

  case class MostRecentlyViewedTimestamp(endpoint: String, sTVV: VectorTime)

  case object Enable

  case object TriggerStabilityCheck

  case object TriggerLightweightStabilityCheck

  case class TCStable(timestamps: Set[VectorTime]) // TODO separate in TCStable and TCStableSet?

  class TCStableFilter(stable: VectorTime) extends Function[VectorTime, Boolean] {
    override def apply(v1: VectorTime): Boolean = stable >= v1
  }

  case object VectorTimesUncomparable extends Throwable

  val stabilityCheck: StabilityCheck = matrix => event => matrix.forall(_ >= event)
  val stableEvents: StableEvents = matrix => events => events.filter(stabilityCheck(matrix))

  // TODO Maybe this isn't necessary because VT(A -> 1) <-> VT(B -> 1), so the stabilityCheck will never hold
  // However, this isn't enough and we should check if the VectorTimes are complete, otherwise I could be saying that
  // (A = 1, B = 1) is stable in A with this matrix
  // A -> (A = 1, B = 1)
  // B -> (A = 1, B = 1)
  // because A doesn't know C exist in a sparse network (A <-----> B <----> C)
  val comparableVectorTimes: Seq[VectorTime] => Boolean = vts => vts.map(_.value.keys.toSeq.sorted).toSet.size equals 1
  val safeStableEvents: SafeStableEvents = matrix => events => if (comparableVectorTimes(matrix ++ events)) Success(stableEvents(matrix)(events)) else Failure(VectorTimesUncomparable)

  val stableVectorTime: Seq[VectorTime] => VectorTime = rtm => {
    val processIds = rtm.flatMap(_.value.keys).toSet
    VectorTime(processIds.map(processId => (processId, rtm.map(_.localTime(processId)).reduce[Long](Math.min))).toMap)
  }

  def props(partitions: Set[String]) = Props(new StabilityChecker2(partitions))

}

class StabilityChecker2(partitions: Set[String]) extends Actor {

  import StabilityChecker2._

  var eventsToCheck: Set[VectorTime] = Set.empty
  var recentTimestampsMatrix: Map[String, VectorTime] = partitions.map(_ -> VectorTime.Zero).toMap

  def collectTimestamps: Receive = {
    case WritedEvents(writedEvents) => eventsToCheck ++= writedEvents // TODO trigger here?
    case MostRecentlyViewedTimestamp(endpoint, sTVV) => recentTimestampsMatrix get endpoint match {
      case Some(vectorTime) if (sTVV > vectorTime) => recentTimestampsMatrix += endpoint -> sTVV
      case None                                    => recentTimestampsMatrix += endpoint -> sTVV
      case _                                       => ()
    }
  }

  def enabled: Receive = {
    // TODO Shouldn't answer to sender.
    // a) Persist
    // b) EventBus (or EventStream)
    // c) Send to EventLog
    case TriggerStabilityCheck =>
      val sEvents = stableEvents(recentTimestampsMatrix.values.toSeq)(eventsToCheck)
      if (sEvents.nonEmpty)
        sender ! TCStable(sEvents)
    case TriggerLightweightStabilityCheck => sender ! new TCStableFilter(stableVectorTime(recentTimestampsMatrix.values.toSeq))
  }

  def disabled: Receive = {
    case Enable => context.become(collectTimestamps orElse enabled)
  }

  override def receive = collectTimestamps orElse disabled

}
