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

import akka.actor.{ Actor, ActorLogging }

object StabilityChecker {

  type StabilityCheck = Seq[VectorTime] => VectorTime => Boolean
  type StableEvents = Seq[VectorTime] => Set[VectorTime] => Set[VectorTime]

  case class MostRecentlyViewedTimestamp(endpoint: String, sTVV: VectorTime)

  case class WritedEvents(cTVV: VectorTime, events: Seq[VectorTime])

  case object StabilityCheck

  case class TCStable(timestamps: Seq[VectorTime])

  case object VectorTimeComplete

  val stabilityCheck: StabilityCheck = matrix => event => matrix.forall(_ >= event)
  val stableEvents: StableEvents = matrix => events => events.filter(stabilityCheck(matrix))

  /*
  def stabilityCheck2(matrix: Seq[VectorTime])(event: VectorTime): Boolean = matrix.forall(event <= _)

  def stabilityCheck(matrix: Seq[VectorTime])(events: Seq[VectorTime]): Seq[VectorTime] = {
    events.filter(stabilityCheck2(matrix))
  }
  */

}

// This strings are ids (A,B,C) or host:port?
class StabilityChecker(neighbors: Set[String]) extends Actor with ActorLogging {

  import StabilityChecker._

  // each one of my neighbros has seen all of his neighbors?
  // (for sparse networks, e.g. A -> B -> C, where A doesn't know the existence of C until
  // it receives a VectorTime(c -> ?)
  // While this remains false, I can't trigger stability check
  var mostRecentlyViewedTimestamp = Map.empty[String, VectorTime]
  var events: List[VectorTime] = List.empty[VectorTime]
  var ready = false

  // TODO para que necesito esto?
  var cTVV: VectorTime = VectorTime.Zero // O deberia inicializarlo de otra manera? pasandole como parametro?

  def vectorTimeComplete(cTVV: VectorTime) = cTVV.value.keys.toSet equals neighbors

  override def receive = {
    case MostRecentlyViewedTimestamp(endpoint, sTVV) => mostRecentlyViewedTimestamp get endpoint match {
      case Some(vectorTime) if (sTVV > vectorTime) =>
        log.info(s"Updated $sTVV for endpoint $endpoint")
        mostRecentlyViewedTimestamp += endpoint -> sTVV
      case None =>
        log.info(s"Added $sTVV for endpoint $endpoint")
        mostRecentlyViewedTimestamp += endpoint -> sTVV
      case _ => log.error(s"$endpoint not in map")
    }
    case WritedEvents(cTVV, newEvents) =>
      log.info(s"Adding $newEvents")
      events ++= newEvents
      if (cTVV > this.cTVV) {
        log.info(s"Updated cTVV from ${this.cTVV} to $cTVV")
        this.cTVV = cTVV
      }
      //if (vectorTimeComplete(this.cTVV)) {
      if (ready) {
        log.info("The VectorTime is complete")
        self ! StabilityCheck
      } else log.info("The VectorTime isn't complete")
    case StabilityCheck => {
      // since the message was sent (by itself) and later received, the matrix and the local cTVV could changed
      val e = stableEvents(mostRecentlyViewedTimestamp.values.toSeq)(events.toSet)
      log.info(s"Stable Events: $e")
    }
    case VectorTimeComplete => ready = true

  }

}
