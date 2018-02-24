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

  val membershipAggregateId = "cluster-membership"

  object ConnectedEndpoint {
    def apply(endpointId: String, logId: String): ConnectedEndpoint = ConnectedEndpoint(endpointId, Some(logId))

    def apply(endpointId: String): ConnectedEndpoint = ConnectedEndpoint(endpointId, None)
  }

  case class ConnectedEndpoint(endpointId: String, logId: Option[String])

  case class UnconnectedPartition(logId: String, partitions: Set[String])

  case class EventLogMembership(partitions: Set[String])

  object EventLogMembershipState {
    def apply(p: UnconnectedPartition): EventLogMembershipState = EventLogMembershipState(Set(p.logId), p.partitions, Set.empty)
  }

  case class EventLogMembershipState(known: Set[String], unknown: Set[String], pending: Set[UnconnectedPartition]) {

    @tailrec
    final def processPartition(newPartition: UnconnectedPartition): EventLogMembershipState = {
      if (unknown.contains(newPartition.logId)) {
        val _known = known + newPartition.logId
        val _unknown = unknown - newPartition.logId ++ newPartition.partitions.filterNot(known.contains)
        val partial = copy(_known, _unknown)

        partial.pending.find(p => partial.unknown.contains(p.logId)) match {
          case Some(p) => partial.copy(pending = partial.pending - p).processPartition(p)
          case None    => partial
        }
      } else
        copy(pending = pending + newPartition)
    }
  }

}

// TODO Note that if there are any changes to the partitions, won't be reflected here
class EventLogMembershipActor(val logId: String, val eventLog: ActorRef, connectedEndpoints: Int) extends EventsourcedActor with PersistOnEvent {

  import EventLogMembershipProtocol._

  override val aggregateId: Option[String] = Some(membershipAggregateId)

  override def id: String = s"cluster-membership-$logId"

  var state: Option[EventLogMembershipState] = None

  var initialPartitions = Set.empty[String]

  // TODO Why I can't stash events?
  var stashed = Set.empty[UnconnectedPartition]

  // TODO improve
  def checkInitial(connected: Set[String]) = {
    if (connected.size equals connectedEndpoints) {
      persist(UnconnectedPartition(logId, initialPartitions)) {
        case Success(_) => ()
        case Failure(_) => // TODO notice EventLog ?
      }
      commandContext.become(Actor.emptyBehavior)
    }
  }

  def receivingInitialPartitions(connected: Set[String]): Receive = {
    case ConnectedEndpoint(endpointId, remoteLogId) =>
      commandContext.become(receivingInitialPartitions(connected + endpointId))
      remoteLogId.foreach(initialPartitions += _)
      checkInitial(connected + endpointId)
  }

  override def onCommand: Receive = receivingInitialPartitions(Set.empty[String])

  def waitingInitialPartitions: Receive = {
    case u: UnconnectedPartition if u.logId eq logId => // TODO is really needed to stash?
      val initialState = EventLogMembershipState(u)
      val _state = stashed.foldLeft(initialState)(_ processPartition _)
      stashed = Set.empty
      state = Some(_state)
      checkAgreement()
      eventContext.become(waitingUnconnectedPartitions)
    case u: UnconnectedPartition => stashed += u
  }

  def waitingUnconnectedPartitions: Receive = {
    case p: UnconnectedPartition if (p.logId ne logId) =>
      state = state.map(_.processPartition(p))
      checkAgreement()
  }

  def checkAgreement() = {
    state.foreach {
      case EventLogMembershipState(members, unknown, _) if unknown.isEmpty =>
        persistOnEvent(EventLogMembership(members))
        eventLog ! EventLogMembership(members) // or onEvent?
      case _ => ()
    }
  }

  override def onEvent: Receive = waitingInitialPartitions

}
