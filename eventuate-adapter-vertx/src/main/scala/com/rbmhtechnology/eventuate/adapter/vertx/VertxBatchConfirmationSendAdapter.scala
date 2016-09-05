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
import io.vertx.core.Vertx

import scala.concurrent.duration._

private[vertx] object VertxBatchConfirmationSendAdapter {

  def props(id: String, eventLog: ActorRef, endpointRouter: VertxEndpointRouter, vertx: Vertx, storageProvider: StorageProvider, batchSize: Int, confirmationTimeout: FiniteDuration): Props =
    Props(new VertxBatchConfirmationSendAdapter(id, eventLog, endpointRouter, vertx, storageProvider, batchSize, confirmationTimeout))
}

private[vertx] class VertxBatchConfirmationSendAdapter(val id: String, val eventLog: ActorRef, val endpointRouter: VertxEndpointRouter, val vertx: Vertx, val storageProvider: StorageProvider, batchSize: Int, val confirmationTimeout: FiniteDuration)
  extends VertxReadAdapter[Long, Long] with AtLeastOnceDelivery with MessageSender with SequenceNumberProgressStore {

  override def replayBatchSize: Int = batchSize
}
