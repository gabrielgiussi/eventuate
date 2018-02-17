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
import akka.actor.ActorRef
import com.rbmhtechnology.eventuate.EventsourcedActor
import com.rbmhtechnology.eventuate.PersistOnEvent

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success

object EventLogMembershipProtocol {

  case class ActorRefContainer(a: ActorRef) // TODO

  // TODO me interesa saber quien soy yo?
  case class InitialPartitions(id: String, partitions: Set[String])

  case class InitialState(state: EventLogMembershipState)

  case class ConnectedEndpoint(endpoingId: String) // doesn't share the eventLog

  case class ConnectedPartition(endpointId: String, logId: String)

  case class UnconnectedPartition(logId: String, neighbors: Set[String])

  case class EventLogMembership(partitions: Set[String])

  case class PartialPartitions()

  case class GossipPartition(partition: UnconnectedPartition)

  object EventLogMembershipState {
    def apply(p: InitialPartitions): EventLogMembershipState = EventLogMembershipState(p.partitions + p.id, Set(p.id), p.partitions, Set.empty)
  }

  // TODO I think members is not needed, I could use known
  case class EventLogMembershipState(members: Set[String], known: Set[String], unknown: Set[String], pending: Set[UnconnectedPartition]) {

    @tailrec
    final def processPartition(newPartition: UnconnectedPartition): EventLogMembershipState = {
      if (unknown.contains(newPartition.logId)) {
        val _members = members ++ (newPartition.neighbors + newPartition.logId)
        val _known = known + newPartition.logId
        val _unknown = unknown - newPartition.logId ++ newPartition.neighbors.filterNot(known.contains)
        val partial = copy(_members, _known, _unknown)

        partial.pending.find(p => partial.unknown.contains(p.logId)) match {
          case Some(p) => partial.copy(pending = partial.pending - p).processPartition(p)
          case None    => partial
        }
      } else
        copy(pending = pending + newPartition)
    }
  }

  @tailrec
  def processPartition(state: EventLogMembershipState)(newPartition: UnconnectedPartition): EventLogMembershipState = {
    if (state.unknown.contains(newPartition.logId)) {
      val members = state.members ++ (newPartition.neighbors + newPartition.logId)
      val known = state.known + newPartition.logId
      val unknown = state.unknown - newPartition.logId ++ newPartition.neighbors.filterNot(known.contains)
      val partial = state.copy(members, known, unknown)

      partial.pending.find(p => partial.unknown.contains(p.logId)) match {
        case Some(pending) => processPartition(partial.copy(pending = partial.pending - pending))(pending)
        case None          => partial
      }
    } else
      state.copy(pending = state.pending + newPartition)
  }

}

// Note that if there are any changes to the partitions, won't be reflected here
class EventLogMembershipActor(val logId: String, val eventLog: ActorRef, connectedEndpoints: Int) extends EventsourcedActor with PersistOnEvent {

  import EventLogMembershipProtocol._

  override val aggregateId: Option[String] = Some("cluster-membership") // THIS must contain the logName? (I think there is no replication between EvenLogso I should be fine)

  override def id: String = s"cluster-membership-$logId"

  var state: Option[EventLogMembershipState] = None

  var connected = Set.empty[String]

  var initialPartitions = Set.empty[String]

  // TODO Why I can't stash events?
  var stashed = Set.empty[UnconnectedPartition]

  def checkInitial() = {
    if (connected.size equals connectedEndpoints) {
      persist(InitialPartitions(logId, initialPartitions)) {
        case Success(_) => ()
        case Failure(_) => // TODO notice EventLog ?
      }
      persist(GossipPartition(UnconnectedPartition(logId, initialPartitions))) {
        case Success(_) => ()
        case Failure(_) => // TODO notice EventLog ?
      }
      commandContext.become(Actor.emptyBehavior)
    }
  }
  override def onCommand: Receive = {
    // TODO this won't be needed when is restarted (event replication)
    case ConnectedEndpoint(endpointId) =>
      connected += endpointId
      checkInitial()
    case ConnectedPartition(endpointId, remoteLogId) =>
      connected += endpointId
      initialPartitions += remoteLogId
      checkInitial()
  }

  def waitingInitialPartitions: Receive = {
    case i: InitialPartitions if (i.id eq logId) =>
      val initialState = EventLogMembershipState(i)
      val _state = stashed.foldLeft(initialState)(_ processPartition _)
      stashed = Set.empty
      state = Some(_state)
      eventContext.become(waitingUnconnectedPartitions)
    case u: UnconnectedPartition => stashed += u
  }

  def waitingUnconnectedPartitions: Receive = {
    case GossipPartition(p) if (p.logId ne logId) =>
      state = state.map(_.processPartition(p))
      state.foreach {
        case EventLogMembershipState(members, _, unknown, _) if unknown.isEmpty =>
          persistOnEvent(EventLogMembership(members))
          eventLog ! EventLogMembership(members) // or onEvent?
        case _ => ()
      }
  }

  override def onEvent: Receive = waitingInitialPartitions

}
