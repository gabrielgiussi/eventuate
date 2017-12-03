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
import akka.actor.Props
import akka.actor.Terminated
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.log.StabilityChecker.SubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityChecker.TCStable
import com.rbmhtechnology.eventuate.log.StabilityChecker.UnsubscribeTCStable

import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

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

object StabilityChecker {

  type StabilityCheck = Seq[VectorTime] => VectorTime => Boolean
  type StableEvents = Seq[VectorTime] => Set[VectorTime] => Set[VectorTime]
  type SafeStableEvents = Seq[VectorTime] => Set[VectorTime] => Try[Set[VectorTime]]

  object MostRecentlyViewedTimestamps {

    def apply(endpoint: String, timestamp: VectorTime): MostRecentlyViewedTimestamps = MostRecentlyViewedTimestamps(Map(endpoint -> timestamp))

  }

  case class MostRecentlyViewedTimestamps(timestamps: Map[String, VectorTime])

  case object Enable

  case object TriggerStabilityCheck

  // TODO add here the Delay to notify subscribers?
  // TODO What is the cost of adding Multiple schedulings?
  case class SubscribeTCStable(actor: ActorRef)

  case class UnsubscribeTCStable(actor: ActorRef)

  case class ScheduleStabilityNotification(delay: FiniteDuration)

  /**
   *
   * @param stableVector Is the point-wise minimum of all the vectors in the RTM
   */
  case class TCStable(stableVector: VectorTime) {
    def isStable(that: VectorTime): Boolean = that < stableVector
  }

  case object VectorTimesUncomparable extends Throwable

  val stabilityCheck: StabilityCheck = matrix => event => matrix.forall(_ >= event)
  val stableEvents: StableEvents = matrix => events => events.filter(stabilityCheck(matrix))

  // TODO Maybe this isn't necessary because VT(A -> 1) <-> VT(B -> 1), so the stabilityCheck will never hold
  // However, this isn't enough and we should check if the VectorTimes are complete, otherwise I could be saying that
  // (A = 1, B = 1) is stable in A with this matrix
  // A -> (A = 1, B = 1)
  // B -> (A = 1, B = 1)
  // because A doesn't know C exist in a sparse network (A <-----> B <----> C)
  val comparableVectorTimes: Seq[VectorTime] => Boolean = vts => vts.map(_.value.keys.toSeq.sorted).toSet.size equals 1
  val safeStableEvents: SafeStableEvents = matrix => events => if (comparableVectorTimes(matrix ++ events)) Success(stableEvents(matrix)(events)) else Failure(VectorTimesUncomparable)

  val stableVectorTime: Seq[VectorTime] => VectorTime = rtm => {
    val processIds = rtm.flatMap(_.value.keys).toSet
    VectorTime(processIds.map(processId => (processId, rtm.map(_.localTime(processId)).reduce[Long](Math.min))).toMap)
  }

  def props(partitions: Set[String]) = Props(new StabilityChecker(partitions))

}

class StabilityChannel extends Actor {

  def unsubscribe(subscribers: Set[ActorRef], actor: ActorRef) = context.become(subscriptionProtocol(subscribers - actor))

  def subscriptionProtocol(subscribers: Set[ActorRef]): Receive = {
    case SubscribeTCStable(actor) =>
      context.watch(actor)
      context.become(subscriptionProtocol(subscribers + actor))
    case Terminated(actor)          => unsubscribe(subscribers, actor) // TODO
    case UnsubscribeTCStable(actor) => unsubscribe(subscribers, actor)
    case tcs: TCStable              => subscribers.foreach(_ ! tcs)
  }

  override def receive = subscriptionProtocol(Set.empty)

}

class StabilityChecker(partitions: Set[String]) extends Actor with ActorLogging {

  import StabilityChecker._

  var subscriptors: Set[ActorRef] = Set.empty
  var recentTimestampsMatrix: Map[String, VectorTime] = partitions.map(_ -> VectorTime.Zero).toMap

  def receiveSubscriptions: Receive = {
    case SubscribeTCStable(subscriptor) =>
      subscriptors += subscriptor
      context.watch(subscriptor)
    case Terminated(subscriptor)          => self ! UnsubscribeTCStable(subscriptor)
    case UnsubscribeTCStable(subscriptor) => subscriptors -= subscriptor
  }

  def receiveTimestamps: Receive = {
    case MostRecentlyViewedTimestamps(timestamps) =>
      log.info(s"Updating RTM with $timestamps")
      timestamps.foreach {
        case (endpoint, sTVV) =>
          // TODO aca falta la logica Georges Younes!
          recentTimestampsMatrix get endpoint match {
            case Some(vectorTime) if (sTVV > vectorTime) => recentTimestampsMatrix += endpoint -> sTVV
            //case None                                    => recentTimestampsMatrix += endpoint -> sTVV
            case None                                    => throw new Exception("Bad partitions configuration")
            case _                                       => ()
          }
      }
      log.info(s"Updated RTM $recentTimestampsMatrix")
  }

  def enabled: Receive = {
    case TriggerStabilityCheck =>
      val stable = stableVectorTime(recentTimestampsMatrix.values.toSeq)
      //TODO Should this be a Stream to avoid sending duplicates to subscribers?
      if (!stable.equiv(VectorTime.Zero)) sender() ! TCStable(stable) //subscriptors.foreach(_ ! stable) FIXME this shouldn't be here
  }

  def disabled: Receive = {
    case Enable => context.become(receiveTimestamps orElse enabled)
  }

  override def receive = receiveTimestamps orElse enabled orElse receiveSubscriptions

}
