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
import akka.stream.ClosedShape
import akka.stream.FanInShape2
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.log.StabilityProtocol.MostRecentlyViewedTimestamps
import com.rbmhtechnology.eventuate.log.StabilityProtocol.RTM
import com.rbmhtechnology.eventuate.log.StabilityProtocol.StableVT
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable

object StabilityProtocol {

  object MostRecentlyViewedTimestamps {
    def apply(id: String, timestamp: VectorTime): MostRecentlyViewedTimestamps = MostRecentlyViewedTimestamps(Map(id -> timestamp))
  }
  case object StableVT
  case object TriggerStability
  case class MostRecentlyViewedTimestamps(timestamps: Map[String, VectorTime])
  type RTM = Map[String, VectorTime]
  case class TCStable(private val stableVector: VectorTime) {
    def isStable(that: VectorTime): Boolean = that <= stableVector
  }

  val updateRTM: RTM => Map[String, VectorTime] => RTM = rtm => timestamps => {
    val updated = timestamps.foldLeft(rtm) {
      case (acc, (endpoint, sTVV)) =>
        acc.get(endpoint) match {
          case Some(vectorTime) => acc + (endpoint -> sTVV.merge(vectorTime))
          case None             => acc + (endpoint -> sTVV)
          // case None => throw Exception
          case _                => acc
        }
    }
    val merged = timestamps.tail.values.fold(timestamps.head._2)(_ merge _)
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

class StabilityStream {

  /*
  def rtmFlow(partitions: Set[String]) = GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._
    val updates = b.add(Flow.apply[MostRecentlyViewedTimestamps])
    val ask = b.add(Flow.apply[StableVT.type])
    val merge = b.add(Merge[Any](2)) // TODO eager? merge preferred?
    ask ~> merge
    updates ~> merge
    val rtm = b.add(Flow.apply[Any].statefulMapConcat(() => {
      var m: RTM = partitions.map(_ -> VectorTime.Zero).toMap
      in => in match {
        case MostRecentlyViewedTimestamps(timestamps) =>
          m = StabilityProtocol.updateRTM(m)(timestamps)
          List()
        case StableVT => List(StabilityProtocol.stableVectorTime(m.values.toSeq))
      }
    }))
    merge ~> rtm

    new FanInShape2(updates.in, ask.in, rtm.out)
  }

  private val hub = BroadcastHub.sink[TCStable](bufferSize = 256)
  private val updates = Source.actorRef[MostRecentlyViewedTimestamps](1, OverflowStrategy.dropHead)
  private val ask = Source.actorRef[StableVT.type](1, OverflowStrategy.dropNew)

  def deduplicate[A] = Flow.apply[A].statefulMapConcat(() => {
    var state: Option[A] = None
    in => in match {
      case Some(i: A) if !(i equals state) =>
        state = Some(i)
        List(i)
      case _ => List()
    }
  })

  def g(actor: ActorRef) = {
    import akka.pattern.ask
    import scala.concurrent.duration._
    implicit val timeout = new Timeout(1 second)
    val a = Source.actorRef(1, OverflowStrategy.dropHead)
      .map(m => actor ! m)
      .mapAsync(1)(_ => actor ? StableVT)
      .via(deduplicate)
      .toMat(BroadcastHub.sink(bufferSize = 256))(Keep.right)
    a
  }

  def graph(partitions: Set[String], state: ActorRef) = RunnableGraph.fromGraph(GraphDSL.create(updates, ask, rtmFlow(partitions), hub)((u, a, _, h) => (u, a, h)) { implicit b => (updates, ask, rtm, hub) =>
    import GraphDSL.Implicits._
    updates ~> rtm.in0
    ask ~> rtm.in1
    rtm.out ~> hub
    ClosedShape
  })

*/
}
