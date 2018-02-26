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

package com.rbmhtechnology.eventuate.crdt

import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated
import com.rbmhtechnology.eventuate.EventsourcedView
import com.rbmhtechnology.eventuate.EventsourcedView.Handler
import com.rbmhtechnology.eventuate.log.StabilityChecker
import com.rbmhtechnology.eventuate.log.StabilityProtocol.MostRecentlyViewedTimestamps
import com.rbmhtechnology.eventuate.log.StabilityProtocol.StableVT
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable

object TCSBActor {

  case object SaveSnapshot

  case class SubscribeTCStable(actor: ActorRef)

  case class UnsubscribeTCStable(actor: ActorRef)

  def props(serviceId: String, eventLog: ActorRef, localPartition: String, partitions: Set[String]) = Props(new TCSBActor(serviceId, eventLog, localPartition, partitions))
}

class TCSBActor(serviceId: String, val eventLog: ActorRef, localPartition: String, partitions: Set[String]) extends EventsourcedView {

  import TCSBActor._

  override def id: String = s"tcsb-$serviceId"

  var lastTCStable: Option[TCStable] = None

  val checker = context.actorOf(StabilityChecker.props(partitions))

  def unsubscribe(subscribers: Set[ActorRef], actor: ActorRef) = {
    context.unwatch(actor)
    commandContext.become(subscriptionProtocol(subscribers - actor))
  }

  def subscriptionProtocol(subscribers: Set[ActorRef]): Receive = {
    case SubscribeTCStable(actor) =>
      lastTCStable.foreach(actor ! _)
      context.watch(actor)
      commandContext.become(subscriptionProtocol(subscribers + actor))
    case Terminated(actor) =>
      unsubscribe(subscribers, actor)
    case UnsubscribeTCStable(actor) =>
      unsubscribe(subscribers, actor)
    case tcs: TCStable if !tcs.isZero() && (lastTCStable.fold(true)(!_.equiv(tcs))) =>
      lastTCStable = Some(tcs)
      subscribers.foreach(_ ! tcs)
    case TCStable(_) => ()
    case SaveSnapshot =>
      save(lastTCStable)(Handler.empty)
  }

  override def onCommand: Receive = subscriptionProtocol(Set.empty)

  override def onEvent: Receive = {
    case _ if lastProcessId != localPartition =>
      checker ! MostRecentlyViewedTimestamps(lastProcessId, lastVectorTimestamp)
      checker ! StableVT
  }

  override def onSnapshot: Receive = {
    case t: Some[TCStable] => lastTCStable = t
  }
}
