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
import akka.actor.ActorRef
import akka.actor.Terminated
import com.rbmhtechnology.eventuate.log.StabilityChannel.SubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityChannel.UnsubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable

object StabilityChannel {

  case class SubscribeTCStable(actor: ActorRef)
  case class UnsubscribeTCStable(actor: ActorRef)

}

class StabilityChannel extends Actor with ActorLogging {

  var lastTCStable: Option[TCStable] = None

  def unsubscribe(subscribers: Set[ActorRef], actor: ActorRef) = context.become(subscriptionProtocol(subscribers - actor))

  def subscriptionProtocol(subscribers: Set[ActorRef]): Receive = {
    case SubscribeTCStable(actor) =>
      context.watch(actor)
      context.become(subscriptionProtocol(subscribers + actor))
    case Terminated(actor)          => unsubscribe(subscribers, actor)
    case UnsubscribeTCStable(actor) => unsubscribe(subscribers, actor)
    case tcs: TCStable if !tcs.isZero() && (lastTCStable.fold(true)(!_.equiv(tcs))) =>
      lastTCStable = Some(tcs)
      subscribers.foreach(_ ! tcs)
    case TCStable(_) => ()
  }

  override def receive = subscriptionProtocol(Set.empty)

}
