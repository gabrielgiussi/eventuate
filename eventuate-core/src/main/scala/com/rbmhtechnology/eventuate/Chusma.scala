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

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.pattern.{ ask, pipe }
import akka.util.Timeout
import com.rbmhtechnology.eventuate.Chusma.{ Forward, Response }
import com.rbmhtechnology.eventuate.ReplicationProtocol.{ ReplicationRead, ReplicationReadFailure, ReplicationReadSuccess }

object Chusma {
  case class Forward(read: ReplicationRead, eventLog: ActorRef)
  case class Response(result: Either[ReplicationReadSuccess, ReplicationReadFailure], remoteReplicator: ActorRef)

  def props(neighbors: Set[String]) = Props(new Chusma(neighbors))
}

class Chusma(neighbors: Set[String]) extends Actor with ActorLogging {

  import context.dispatcher
  import scala.concurrent.duration._
  implicit val timeout = Timeout.apply(5.seconds)
  var seen = Set.empty[String] // TODO this isn't enough for sparse networks. Remote Endpoint should tell me if they
  // have seen all of his own neighboors (maybe
  implicit def actorRefToString(ref: ActorRef): Option[String] = for {
    port <- ref.path.address.port
    host <- ref.path.address.host
  } yield s"neighbor[$host:$port]"

  override def receive = {
    case Forward(read, eventLog) => {
      log.info("Prueba received Forward")
      val remoteReplicator = sender()
      // if the vectorTime already has C for example, it doesn't care if on restart i receive a ReplicationRead from C
      // the problem is that i'm using host and port to identify neighbors but in VectorTime i'm endpoint ID
      // this is because in eventuate configuration, the remote endpoints are configurated using host:port and
      // i don't know the relation endpointId <-> endpoint host:port
      actorRefToString(remoteReplicator).foreach { seen += _ }
      val seenAllNeighbors = seen equals neighbors
      log.error(s"Seen all neighbors? $seenAllNeighbors")
      val a = eventLog ? read map { // What happens if I have a timeout here?
        case r: ReplicationReadSuccess => Response(Left(r.copy(neighborsViewed = seenAllNeighbors)), remoteReplicator)
        case r: ReplicationReadFailure => Response(Right(r), remoteReplicator)
        case _                         => log.error("Message not expected")
      }
      a pipeTo self
    }
    case Response(s, replicator) => {
      val result = s.fold(a => a, b => b)
      log.info(s"Received Response(result = $result, actor = $replicator)")
      replicator ! result
    }
  }
}
