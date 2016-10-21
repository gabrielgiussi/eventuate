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
import scala.util.Either

case class EnhancedEntry[A](entry: Versioned[A], processId: String)

case class Match[A](versionedEntries: Set[Versioned[A]] = Set.empty[Versioned[A]]) extends CRDTFormat {

  def value: Set[A] =
    versionedEntries.map(_.value)

  def add(entry: A, timestamp: VectorTime, processId: String): Either[(Match[A], Versioned[A]), Match[A]] = {
    val newVersion = Versioned(entry, timestamp, processId = processId)
    val newVersionedEntries = versionedEntries + newVersion
    if (versionedEntries.size < Match.MATCH_SIZE) {
      val newMatch = copy(newVersionedEntries)
      Right(newMatch)
    } else {
      val removed = newVersionedEntries.toVector.sorted(Match.MatchOrdering[A]).last
      if (removed equals entry)
        Left(this, removed)
      else
        Left(copy(newVersionedEntries - removed), removed)
    }

  }

  def prepareRemove(entry: A): Set[VectorTime] =
    versionedEntries.collect { case Versioned(`entry`, timestamp, _, _, _) => timestamp }

  def remove(timestamps: Set[VectorTime]): Match[A] =
    copy(versionedEntries.filterNot(versionedEntry => timestamps.contains(versionedEntry.vectorTimestamp)))

}

object Match {
  def apply[A]: Match[A] =
    new Match[A]()

  val MATCH_SIZE = 3

  implicit def MatchServiceOps[A] = new CRDTServiceOps[Match[A], Set[A]] {
    override def zero: Match[A] =
      Match.apply[A]

    override def value(crdt: Match[A]): Set[A] =
      crdt.value

    override def prepare(crdt: Match[A], operation: Any): Option[Any] = operation match {
      case op @ RemoveOp(entry, _) => crdt.prepareRemove(entry.asInstanceOf[A]) match {
        case timestamps if timestamps.nonEmpty =>
          Some(op.copy(timestamps = timestamps))
        case _ =>
          None
      }
      case op @ AddOp(e) if (crdt.versionedEntries.size < MATCH_SIZE) => super.prepare(crdt, op)
      case _ => None
    }

    override def update(crdt: Match[A], operation: Any, event: DurableEvent): (Match[A], Option[Apologie[Set[A]]]) = operation match {
      case RemoveOp(_, timestamps) => (crdt.remove(timestamps), None)
      case AddOp(entry) =>
        crdt.add(entry.asInstanceOf[A], event.vectorTimestamp, event.processId) match {
          case Left((m, e)) => (m, Some(new Apologie(e.value toString, operation)))
          case Left((m, _)) => (m, None)
          case Right(m)     => (m, None)
        }
    }
  }

  implicit def MatchOrdering[A] = new Ordering[Versioned[A]] {
    override def compare(x: Versioned[A], y: Versioned[A]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        //x.creator.compareTo(y.creator)
        x.value.toString.compareTo(y.value.toString)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }
}

class MatchService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[Match[A], Set[A]])
  extends CRDTService[Match[A], Set[A]] {

  /**
   * Adds `entry` to the OR-Set identified by `id` and returns the updated entry set.
   */
  def add(id: String, entry: A): Future[Set[A]] = {
    op(id, AddOp(entry))
  }

  /**
   * Removes `entry` from the OR-Set identified by `id` and returns the updated entry set.
   */
  def remove(id: String, entry: A): Future[Set[A]] =
    op(id, RemoveOp(entry))

  start()
}

private[eventuate] case class AddOp(entry: Any) extends CRDTFormat

private[eventuate] case class RemoveOp(entry: Any, timestamps: Set[VectorTime] = Set.empty) extends CRDTFormat
