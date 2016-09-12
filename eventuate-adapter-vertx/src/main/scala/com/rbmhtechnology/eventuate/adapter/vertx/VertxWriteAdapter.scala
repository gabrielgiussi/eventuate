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

package com.rbmhtechnology.eventuate.adapter.vertx

import akka.actor.{ ActorRef, Props }
import com.rbmhtechnology.eventuate.EventsourcedActor
import io.vertx.core.eventbus.Message
import io.vertx.core.{ Vertx, Handler => VertxHandler }

import scala.util.{ Failure, Success }

private[vertx] object VertxWriteAdapter {

  case class PersistEvent(event: Any, message: Message[Any])

  def props(id: String, eventLog: ActorRef, endpoint: String, vertx: Vertx): Props =
    Props(new VertxWriteAdapter(id, eventLog, endpoint, vertx))
}

private[vertx] class VertxWriteAdapter(val id: String, val eventLog: ActorRef, endpoint: String, vertx: Vertx) extends EventsourcedActor {

  import VertxWriteAdapter._

  var messageConsumer = vertx.eventBus().localConsumer[Any](endpoint, new VertxHandler[Message[Any]] {
    override def handle(message: Message[Any]): Unit = {
      self ! PersistEvent(message.body(), message)
    }
  })

  override def stateSync: Boolean = false

  override def onCommand: Receive = {
    case PersistEvent(evt, msg) =>
      persist(evt) {
        case Success(res) => msg.reply(res)
        case Failure(err) => msg.fail(0, err.getMessage)
      }
  }

  override def onEvent: Receive = {
    case _ =>
  }

  override def postStop(): Unit = {
    super.postStop()
    messageConsumer.unregister()
  }
}
