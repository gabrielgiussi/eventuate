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
import com.rbmhtechnology.eventuate.crdt.CRDT.EnhancedNonCommutativeCRDT
import com.rbmhtechnology.eventuate.crdt.CRDT.SimpleCRDT
import com.rbmhtechnology.eventuate.crdt.CRDTTypes._

import scala.concurrent.Future

/**
 * [[AWCart]] entry.
 *
 * @param key      Entry key. Used to identify a product in the shopping cart.
 * @param quantity Entry quantity.
 * @tparam A Key type.
 */
case class AWCartEntry[A](key: A, quantity: Int) extends CRDTFormat

object AWCart {
  def apply(): SimpleCRDT = ORCartServiceOps.zero

  implicit class AWCartCRDT[A](crdt: SimpleCRDT) extends EnhancedNonCommutativeCRDT(crdt) {

    def add(key: A, quantity: Int, timestamp: VectorTime)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, AddOp(AWCartEntry(key, quantity)), timestamp)
    def remove(key: A, t: VectorTime)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, RemoveOp(key), t)
    def clear(t: VectorTime)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, Clear, t)
  }

  implicit def ORCartServiceOps[A] = new CRDTNonCommutativePureOpSimple[Map[A, Int]] {

    override def customEval(ops: Seq[Versioned[Operation]]): Map[A, Int] = ops.foldLeft(Map.empty[A, Int]) {
      case (acc, Versioned(AddOp(AWCartEntry(key: A, quantity)), _, _, _)) => acc.get(key) match {
        case Some(c) => acc + (key -> (c + quantity))
        case None    => acc + (key -> quantity)
      }
      case (acc, Versioned(RemoveOp(_, _), _, _, _)) => acc
    }

    val r: R = (v, _) => v.value match {
      case _: RemoveOp => true
      case Clear       => true
      case _           => false
    }

    val r0: R_ = newOp => op => {
      ((op.vectorTimestamp, op.value), (newOp.vectorTimestamp, newOp.value)) match {
        case ((t1, AddOp(AWCartEntry(k1, _))), (t2, RemoveOp(k2, _))) => (t1 < t2) && (k1 equals k2)
        case ((t1, AddOp(_)), (t2, Clear)) => (t1 < t2)
        case _ => false
      }
    }

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)
  }
}

/**
 * Replicated [[AWCart]] CRDT service.
 *
 *  - For adding a new `key` of given `quantity` a client should call `add`.
 *  - For incrementing the `quantity` of an existing `key` a client should call `add`.
 *  - For decrementing the `quantity` of an existing `key` a client should call `remove`, followed by `add`
 * (after `remove` successfully completed).
 *  - For removing a `key` a client should call `remove`.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @tparam A [[AWCart]] key type.
 */
class AWCartService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTNonCommutativePureOpSimple[Map[A, Int]])
  extends CRDTService[SimpleCRDT, Map[A, Int]] {

  /**
   * Adds the given `quantity` of `key` to the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def add(id: String, key: A, quantity: Int): Future[Map[A, Int]] =
    if (quantity > 0) op(id, AddOp(AWCartEntry(key, quantity))) else Future.failed(new IllegalArgumentException("quantity must be positive"))

  /**
   * Removes the given `key` from the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def remove(id: String, key: A): Future[Map[A, Int]] =
    op(id, RemoveOp(key))

  def clear(id: String): Future[Map[A, Int]] =
    op(id, Clear)

  start()
}
