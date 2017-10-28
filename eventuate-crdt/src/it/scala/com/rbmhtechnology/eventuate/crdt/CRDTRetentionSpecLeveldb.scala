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

import akka.actor.{ ActorRef, ActorSystem }
import akka.pattern._
import akka.testkit.{ TestKit, TestProbe }
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import org.scalatest.{ Matchers, WordSpecLike }

object CRDTRetentionSpecLeveldb {

  case class CRDTStopped(id: String)

  case class CRDTCreated(id: String, ref: ActorRef)

}

class CRDTRetentionSpecLeveldb extends TestKit(ActorSystem("test")) with WordSpecLike with Matchers with SingleLocationSpecLeveldb {
  import CRDTRetentionSpecLeveldb._
  import scala.concurrent.duration._

  val probe = TestProbe()
  var lastCreated: ActorRef = _

  def service(serviceId: String) = new CounterService[Int](serviceId, log) {
    override private[crdt] def onStop(id: String): Unit = probe.ref ! CRDTStopped(id)

    override private[crdt] def onCreation(id: String, ref: ActorRef): Unit = {
      lastCreated = ref
      probe.ref ! CRDTCreated(id, ref)
    }

  }

  val service = new CounterService[Int]("a", log)

  "A CRDTService" must {
    "stop CRDTActors after 1 minute" in {

      service.update("Counter1", 1)
      probe.expectMsg(CRDTCreated("Counter1", _))

      probe.expectTerminated(lastCreated, 70 seconds)

    }
  }

}
