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

import java.util.function.BiConsumer

import com.rbmhtechnology.eventuate.EventsourcedView
import com.rbmhtechnology.eventuate.adapter.vertx.japi.{LogAdapterService => JLogAdapterService}
import io.vertx.core.{AsyncResult, Handler, Vertx}

import scala.util.{Failure, Success}

object LogAdapterService {
  type EventHandler[A <: Event] = (A, EventSubscription) => Unit

  def apply(logName: String, vertx: Vertx): LogAdapterService[Event] =
    new LogAdapterService[Event](JLogAdapterService.create(logName, vertx))

  def apply(logName: String, consumer: String, vertx: Vertx): LogAdapterService[ConfirmableEvent] =
    new LogAdapterService[ConfirmableEvent](JLogAdapterService.create(logName, consumer, vertx))

  private[vertx] def apply(logAdapterInfo: LogAdapterInfo, vertx: Vertx): LogAdapterService[Event] =
    new LogAdapterService[Event](JLogAdapterService.create(logAdapterInfo, vertx))

  private[vertx] def apply(logAdapterInfo: SendLogAdapterInfo, vertx: Vertx) : LogAdapterService[ConfirmableEvent] =
    new LogAdapterService[ConfirmableEvent](JLogAdapterService.create(logAdapterInfo, vertx))
}

class LogAdapterService[A <: Event] private[eventuate](delegate: JLogAdapterService[A]) {

  import LogAdapterService._

  def onEvent(handler: EventHandler[A]): EventSubscription = {
    delegate.onEvent(new BiConsumer[A, EventSubscription] {
      override def accept(event: A, sub: EventSubscription): Unit = handler.apply(event, sub)
    })
  }

  def persist[E](event: E)(handler: EventsourcedView.Handler[E]): Unit = {
    delegate.persist(event, new Handler[AsyncResult[E]] {
      override def handle(event: AsyncResult[E]): Unit = {
        if (event.succeeded()) {
          handler(Success(event.result()))
        } else {
          handler(Failure(event.cause()))
        }
      }
    })
  }
}