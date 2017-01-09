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
import com.rbmhtechnology.eventuate.{ DurableEvent, VectorTime, Versioned }

import scala.collection.immutable.Set
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

case class CERMatch(orSet: ORSet[String] = ORSet.apply[String]) extends CRDTFormat {

  def value: Set[String] = orSet.value

  def add(player: String, timestamp: VectorTime): (CERMatch, Option[Versioned[String]]) = {
    val updatedOrSet = orSet.add(player, timestamp)
    if (orSet.value.size < CERMatch.MATCH_SIZE) {
      println("Player agregado: " + player)
      (copy(updatedOrSet), None)
    } else {
      val removed = updatedOrSet.versionedEntries.filter(x => x.vectorTimestamp.conc(timestamp) || x.value.equals(player)).toVector.sorted(CERMatch.CERMatchOrdering).last
      println("Removed: " + removed.value)
      if (!removed.value.equals(player)) {
        println("Apology: " + removed)
        (copy(updatedOrSet.remove(updatedOrSet.prepareRemove(removed.value))), Some(removed))
      } else {
        println("No me pienso disculpar " + player + " == " + removed.value)
        (copy(updatedOrSet.remove(updatedOrSet.prepareRemove(removed.value))), None)
      }
    }
  }

  def prepareRemove(entry: String): Set[VectorTime] = orSet.prepareRemove(entry)

  def remove(timestamps: Set[VectorTime]): CERMatch = copy(orSet.remove(timestamps))
}

object CERMatch {
  def apply(): CERMatch =
    new CERMatch()

  val MATCH_SIZE = 3

  val orSetOps = ORSet.ORSetServiceOps[String]

  implicit def CERMatchOrdering = new Ordering[Versioned[String]] {
    override def compare(x: Versioned[String], y: Versioned[String]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        x.value.toString.compareTo(y.value.toString)
      //x.creator.compareTo(y.creator)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }

  implicit def CERMatchServiceOps = new CRDTServiceOps[CERMatch, Set[String]] {
    override def zero: CERMatch =
      CERMatch.apply()

    override def value(crdt: CERMatch): Set[String] = crdt.value

    override def prepare(crdt: CERMatch, operation: Any): Try[Option[Any]] = operation match {
      //case op @ RemoveOp(entry, _) => ??
      case op @ AddOp(e) if (crdt.value.size < MATCH_SIZE) => orSetOps.prepare(crdt.orSet, operation)
      case _ => Failure(new Exception("Match is already complete"))
    }

    override def effect(crdt: CERMatch, operation: Any, event: DurableEvent): (CERMatch, Option[Apology]) = operation match {
      case RemoveOp(_, timestamps) => (crdt.remove(timestamps), None)
      case AddOp(entry) =>
        println("Recibido AddOp " + entry.toString)
        crdt.add(entry.asInstanceOf[String], event.vectorTimestamp) match {
          case (m, Some(removed)) => (m, Some(Apology(removed, Versioned(entry, event.vectorTimestamp))))
          case (m, None)          => (m, None)
        }
    }
  }

}

class MatchService(val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[CERMatch, Set[String]])
  extends CRDTService[CERMatch, Set[String]] {

  /**
   * Adds `entry` to the OR-Set identified by `id` and returns the updated entry set.
   */
  def add(id: String, entry: String): Future[Set[String]] =
    op(id, AddOp(entry))

  /**
   * Removes `entry` from the OR-Set identified by `id` and returns the updated entry set.
   */
  def remove(id: String, entry: String): Future[Set[String]] =
    op(id, RemoveOp(entry))

  start()

}
