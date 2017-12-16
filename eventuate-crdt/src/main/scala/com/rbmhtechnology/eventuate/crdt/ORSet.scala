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

import akka.actor._
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation, PruneState, UpdateState }

import scala.concurrent.Future
import scala.collection.immutable.Set
import scala.util.{ Success, Try }

/**
 * Operation-based OR-Set CRDT. [[Versioned]] entries are uniquely identified with vector timestamps.
 *
 * @param versionedEntries [[Versioned]] entries.
 * @tparam A Entry value type.
 *
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 15
 */
case class ORSet[A](polog: POLog = POLog(), state: Set[A] = Set.empty[A]) extends CRDT[Set[A]] with CRDTHelper[Set[A], ORSet[A]] {
  /**
   * Returns all entries, masking duplicates of different version.
   */

  override protected[crdt] val obs: Obsolete = (op1, op2) => {
    ((op1.vectorTimestamp, op1.value), (op2.vectorTimestamp, op2.value)) match {
      case ((t1, AddOp(v1)), (t2, AddOp(v2)))       => (t1 < t2) && (v1 equals v2)
      case ((t1, AddOp(v1)), (t2, RemoveOp(v2, _))) => (t1 < t2) && (v1 equals v2)
      case ((_, RemoveOp(_, _)), _)                 => true
    }
  }

  override def eval(): Set[A] =
    polog.log.filter(_.value.isInstanceOf[AddOp]).map(_.value.asInstanceOf[AddOp].entry.asInstanceOf[A]) // TODO improve

  override def copyCRDT(polog: POLog, state: Set[A]) = copy(polog, state)

  // TODO this is in some extent, duplicated with what ORSetService does in add! and CRDTServiceOps in effect
  def add(entry: A, vt: VectorTime): ORSet[A] = addOp(AddOp(entry), vt).asInstanceOf[ORSet[A]] // TODO improve

  def remove(entry: A, vt: VectorTime): ORSet[A] = addOp(RemoveOp(entry), vt).asInstanceOf[ORSet[A]] // TODO improve
}

object ORSet {
  def apply[A]: ORSet[A] = ORSet[A]()

  /**
   * The more interesting rule is that any rmv
   * is made obsolete by any other timestamp-operation pair; this implies that a rmv
   * can only exist as the single element of a PO-Log (if it was inserted into an empty
   * PO-Log), being discarded once other operation arrives (including another rmv),
   * and never being inserted into a non-empty PO-Log. (Page 11)
   * @param s
   * @param op
   * @tparam A
   * @return
   */
  def merge[A](s: Set[A], op: Versioned[Operation]) = op.value match {
    case AddOp(e)       => s + e.asInstanceOf
    case RemoveOp(e, _) => s - e.asInstanceOf // FIXME it will be removeop in the set?
  }

  implicit def ORSetServiceOps[A] = new CRDTServiceOps[Set[A]] {
    override def zero: ORSet[A] =
      ORSet.apply[A]

  }
}

//#or-set-service
/**
 * Replicated [[ORSet]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[ORSet]] entry type.
 */
class ORSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[Set[A]])
  extends CRDTService[Set[A]] {

  /**
   * Adds `entry` to the OR-Set identified by `id` and returns the updated entry set.
   */
  def add(id: String, entry: A): Future[Set[A]] =
    op(id, AddOp(entry))

  /**
   * Removes `entry` from the OR-Set identified by `id` and returns the updated entry set.
   */
  def remove(id: String, entry: A): Future[Set[A]] =
    op(id, RemoveOp(entry))

  start()
}

/**
 * Persistent add operation used for [[ORSet]] and [[ORCart]].
 */
case class AddOp(entry: Any) extends CRDTFormat

/**
 * Persistent remove operation used for [[ORSet]] and [[ORCart]].
 */
case class RemoveOp(entry: Any, timestamps: Set[VectorTime] = Set.empty) extends CRDTFormat // FIXME timestamp needed?
