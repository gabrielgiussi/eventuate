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
import com.rbmhtechnology.eventuate.Versioned
import com.rbmhtechnology.eventuate.crdt.CRDTTypes._

import scala.collection.immutable.Set
import scala.concurrent.Future

case class ORSet[A](override val polog: POLog = POLog(), override val state: Set[A] = Set.empty[A]) extends CRDT[Set[A]] with CRDTHelper[Set[A], ORSet[A]] {

  // TODO realmente me sirve que este metodo este en los CRDT?
  override def copyCRDT(polog: POLog, state: Set[A]): ORSet[A] = copy(polog, state)

  override def eval = polog.log.foldLeft(state)((s: Set[A], op: Versioned[Operation]) => op.value match {
    case AddOp(v) => s + v.asInstanceOf[A]
    case _        => s
  })

}

object ORSet {

  def apply[A]() = new ORSet[A]()

  implicit def ORSetServiceOps[A] = new CRDTServiceOps[ORSet[A], Set[A]] {
    override def zero: ORSet[A] = ORSet.apply[A]

    //    override val eval: Eval[Set[A]] = (polog: POLog, state: Set[A]) => polog.log.foldLeft(state)((s: Set[A], op: Versioned[Operation]) => op.value match {
    //      case AddOp(v) => s + v.asInstanceOf[A]
    //    })

    override val obs = (op1: Versioned[Operation], op2: Versioned[Operation]) => (op1, op2) match {
      case (Versioned(AddOp(v), t, _, _), Versioned(AddOp(v2), t2, _, _)) => t.lt(t2) && v == v2
      case (Versioned(AddOp(v), t, _, _), Versioned(RemoveOp(v2), t2, _, _)) => t.lt(t2) && v == v2
      case (_, Versioned(RemoveOp(v), _, _, _)) => true
      case _ => false
    }

    override val pruneState = (op: Operation, s: Set[A], o: Obsolete) => s

    override val updateState = (s: Set[A], op: Versioned[Operation]) => op.value match {
      case AddOp(v) => s + v.asInstanceOf[A]
      // TODO case RemoveOp(v,_) => s - v.asInstanceOf[A] ACA DEBERIA DARLE BOLA AL RemoveOp.timestamps ????
      case _        => s
    }

    override def precondition: Boolean =
      false

  }

}

class ORSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[ORSet[A], Set[A]])
  extends CRDTService[ORSet[A], Set[A]] {

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

case class AddOp(entry: Any) extends CRDTFormat

//case class RemoveOp(entry: Any, timestamps: Set[VectorTime] = Set.empty) extends CRDTFormat
case class RemoveOp(entry: Any) extends CRDTFormat
