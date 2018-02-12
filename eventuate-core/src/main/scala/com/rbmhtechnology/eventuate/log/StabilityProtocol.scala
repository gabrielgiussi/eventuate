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

import com.rbmhtechnology.eventuate.VectorTime

object StabilityProtocol {

  object MostRecentlyViewedTimestamps {
    def apply(id: String, timestamp: VectorTime): MostRecentlyViewedTimestamps = MostRecentlyViewedTimestamps(Map(id -> timestamp))
  }

  case object StableVT

  case class MostRecentlyViewedTimestamps(timestamps: Map[String, VectorTime])

  type RTM = Map[String, VectorTime]

  case class TCStable(private val stableVector: VectorTime) {
    def isStable(that: VectorTime): Boolean = that <= stableVector

    def isZero(): Boolean = stableVector.value.forall(_._2 equals 0L)

    def equiv(that: TCStable): Boolean = stableVector.equiv(that.stableVector)
  }

  val updateRTM: RTM => Map[String, VectorTime] => RTM = rtm => timestamps => {
    val updated = timestamps.foldLeft(rtm) {
      case (acc, (endpoint, sTVV)) =>
        acc.get(endpoint) match {
          case Some(vectorTime) => acc + (endpoint -> sTVV.merge(vectorTime))
          case None             => acc + (endpoint -> sTVV)
          case _                => acc
        }
    }
    val merged = timestamps.values.fold(VectorTime.Zero)(_ merge _)
    val updated2 = merged.value.foldLeft(updated) {
      case (acc, (processId, m)) =>
        val currentVT = acc.getOrElse(processId, VectorTime.Zero)
        val localTime = currentVT.localTime(processId)
        if (m > localTime) acc + (processId -> currentVT.setLocalTime(processId, m))
        else acc
    }
    updated2
  }

  val stableVectorTime: Seq[VectorTime] => TCStable = rtm => {
    val processIds = rtm.flatMap(_.value.keys).toSet
    TCStable(VectorTime(processIds.map(processId => (processId, rtm.map(_.localTime(processId)).reduce[Long](Math.min))).toMap))
  }
}
