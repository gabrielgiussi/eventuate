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

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit._
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import com.typesafe.config.ConfigFactory
import org.scalatest._

import scala.concurrent.duration._

object PublishReadLogAdapterSpec {
  val Config = ConfigFactory.defaultApplication()
    .withFallback(ConfigFactory.parseString(
      """
        |eventuate.log.replay-batch-size = 50
      """.stripMargin))
}

class PublishReadLogAdapterSpec extends TestKit(ActorSystem("test", PublishReadLogAdapterSpec.Config))
  with WordSpecLike with MustMatchers with SingleLocationSpecLeveldb with StopSystemAfterAll with ActorStorage with EventWriter
  with VertxEventbus {

  val inboundLogId = "log_inbound"
  val endpoint = VertxEndpoint("vertx-eb-endpoint")
  var ebProbe: TestProbe = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    registerCodec()
    ebProbe = eventBusProbe(endpoint)
    logAdapter()
  }

  def logAdapter(): ActorRef =
    system.actorOf(PublishReadLogAdapter.props(inboundLogId, log, endpoint, vertx, actorStorageProvider()))

  def read: String = read(inboundLogId)

  def write: (Long) => String = write(inboundLogId)

  "A PublishReadLogAdapter" must {
    "read and publish events from the beginning of the event log" in {
      val writtenEvents = writeEvents("ev", 50)

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      storageProbe.expectMsg(write(50))
      storageProbe.reply(50L)

      storageProbe.expectNoMsg(1.second)

      ebProbe.receiveNVertxMsg[String](50).map(_.body) must be(writtenEvents.map(_.payload))
    }
    "read and publish events from a stored sequence number" in {
      val writtenEvents = writeEvents("ev", 50)

      storageProbe.expectMsg(read)
      storageProbe.reply(10L)

      storageProbe.expectMsg(write(50))
      storageProbe.reply(50L)

      storageProbe.expectNoMsg(1.second)

      ebProbe.receiveNVertxMsg[String](40).map(_.body) must be(writtenEvents.drop(10).map(_.payload))
    }
    "read and publish events in batches" in {
      val writtenEvents = writeEvents("ev", 100)

      storageProbe.expectMsg(read)
      storageProbe.reply(0L)

      storageProbe.expectMsg(write(50))
      storageProbe.reply(50L)

      storageProbe.expectMsg(write(100))
      storageProbe.reply(100L)

      storageProbe.expectNoMsg(1.second)

      ebProbe.receiveNVertxMsg[String](100).map(_.body) must be(writtenEvents.map(_.payload))
    }
  }
}
