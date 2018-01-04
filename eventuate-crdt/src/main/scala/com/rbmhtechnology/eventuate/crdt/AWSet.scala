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
import com.rbmhtechnology.eventuate.crdt.AWSet.AWSet
import com.rbmhtechnology.eventuate.crdt.CRDT.EnhancedNonCommutativeCRDT
import com.rbmhtechnology.eventuate.crdt.CRDTTypes._

import scala.concurrent.Future
import scala.collection.immutable.Set

object AWSet {

  type AWSet[A] = CRDT[Set[A]]

  def apply[A]: AWSet[A] = CRDT(Set.empty) // TODO we should be able to define a specific implementation of Set

  implicit class AWSetCRDT[A](crdt: AWSet[A]) extends EnhancedNonCommutativeCRDT(crdt) {
    def add(value: A, vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOp[_, Set[A]]) = ops.effect(crdt, AddOp(value), vectorTime)
    def remove(value: A, vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOp[_, Set[A]]) = ops.effect(crdt, RemoveOp(value), vectorTime)
    def clear(vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOp[_, Set[A]]) = ops.effect(crdt, Clear, vectorTime)
  }

  implicit def AWSetServiceOps[A] = new CRDTNonCommutativePureOp[Set[A], Set[A]] {

    val r: R = (v, _) => v.value match {
      case _: RemoveOp => true
      case Clear       => true
      case _           => false
    }

    val r0: R_ = newOp => op => {
      ((op.vectorTimestamp, op.value), (newOp.vectorTimestamp, newOp.value)) match {
        case ((t1, AddOp(v1)), (t2, AddOp(v2)))    => (t1 < t2) && (v1 equals v2)
        case ((t1, AddOp(v1)), (t2, RemoveOp(v2))) => (t1 < t2) && (v1 equals v2)
        case ((t1, AddOp(_)), (t2, Clear))         => (t1 < t2)
      }
    }

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)

    override def eval(crdt: AWSet[A]): Set[A] =
      crdt.polog.log.map(_.value.asInstanceOf[AddOp].entry.asInstanceOf[A]) ++ crdt.state

    override protected def stabilizeState(state: Set[A], stableOps: Seq[Operation]): Set[A] =
      state ++ stableOps.map(_.asInstanceOf[AddOp].entry.asInstanceOf[A]).toSet

    override def zero: AWSet[A] = AWSet.apply[A]

    override def updateState(op: Operation, state: Set[A]): Set[A] = op match {
      case RemoveOp(entry) => state - entry.asInstanceOf[A]
      case Clear           => Set.empty
      case _               => state
    }
  }
}

//#or-set-service
/**
 * Replicated [[AWSet]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[AWSet]] entry type.
 */
class AWSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTNonCommutativePureOp[Set[A], Set[A]])
  extends CRDTService[AWSet[A], Set[A]] {

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

  def clear(id: String): Future[Set[A]] =
    op(id, Clear)

  start()

}

/**
 * Persistent add operation used for [[AWSet]] and [[AWCart]].
 */
case class AddOp(entry: Any) extends CRDTFormat

/**
 * Persistent remove operation used for [[AWSet]] and [[AWCart]].
 */
case class RemoveOp(entry: Any) extends CRDTFormat // FIXME timestamp needed?

case object Clear extends CRDTFormat