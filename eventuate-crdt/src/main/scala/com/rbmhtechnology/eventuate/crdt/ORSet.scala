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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

import scala.concurrent.Future
import scala.collection.immutable.Set

object ORSet {
  def apply[A]: CRDT[Set[A]] = CRDT(Set.empty[A])

  implicit class ORSetCRDT[A](crdt: CRDT[Set[A]]) {
    def add(value: A, vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOp[Set[A]]) = ops.effect(crdt, AddOp(value), vectorTime)
    def remove(value: A, vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOp[Set[A]]) = ops.effect(crdt, RemoveOp(value), vectorTime)
  }

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

  implicit def ORSetServiceOps[A] = new CRDTNonCommutativePureOp[Set[A]] {

    override val obs: Obsolete = (op1, op2) => {
      ((op1.vectorTimestamp, op1.value), (op2.vectorTimestamp, op2.value)) match {
        case ((t1, AddOp(v1)), (t2, AddOp(v2)))       => (t1 < t2) && (v1 equals v2)
        case ((t1, AddOp(v1)), (t2, RemoveOp(v2, _))) => (t1 < t2) && (v1 equals v2)
        case ((_, RemoveOp(_, _)), _)                 => true
      }
    }

    override def zero: CRDT[Set[A]] = ORSet.apply[A]

    override def customEval(crdt: CRDT[Set[A]]): Set[A] =
      crdt.polog.log.filter(_.value.isInstanceOf[AddOp]).map(_.value.asInstanceOf[AddOp].entry.asInstanceOf[A])

    override protected def mergeState(stableState: Set[A], evaluatedState: Set[A]): Set[A] = stableState ++ evaluatedState
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
class ORSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[CRDT[Set[A]], Set[A]])
  extends CRDTService[CRDT[Set[A]], Set[A]] {

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
