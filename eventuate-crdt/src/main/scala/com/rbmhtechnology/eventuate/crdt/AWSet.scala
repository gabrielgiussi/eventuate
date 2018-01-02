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
import com.rbmhtechnology.eventuate.crdt.CRDT.EnhancedCRDT
import com.rbmhtechnology.eventuate.crdt.CRDT.EnhancedNonCommutativeCRDT
import com.rbmhtechnology.eventuate.crdt.CRDT.SimpleCRDT
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.CausalRedundancy
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.R
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.R_
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

import scala.concurrent.Future
import scala.collection.immutable.Set

object AWSet {
  def apply(): SimpleCRDT = AWSetServiceOps.zero

  implicit class ORSetCRDT[A](crdt: SimpleCRDT) extends EnhancedNonCommutativeCRDT(crdt) {
    def add(value: A, vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, AddOp(value), vectorTime)
    def remove(value: A, vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, RemoveOp(value), vectorTime)
    def clear(vectorTime: VectorTime)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, Clear, vectorTime)
  }

  implicit def AWSetServiceOps[A] = new CRDTNonCommutativePureOpSimple[Set[A]] {

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

    override def customEval(ops: Seq[Versioned[Operation]]): Set[A] =
      ops.filter(_.value.isInstanceOf[AddOp]).map(_.value.asInstanceOf[AddOp].entry.asInstanceOf[A]).toSet

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)
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
class AWSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTNonCommutativePureOpSimple[Set[A]])
  extends CRDTService[SimpleCRDT, Set[A]] {

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