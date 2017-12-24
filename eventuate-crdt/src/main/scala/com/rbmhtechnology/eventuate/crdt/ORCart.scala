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

import scala.collection.immutable.Set
import scala.concurrent.Future
import scala.util.{ Success, Try }

/**
 * [[ORCart]] entry.
 *
 * @param key      Entry key. Used to identify a product in the shopping cart.
 * @param quantity Entry quantity.
 * @tparam A Key type.
 */
case class ORCartEntry[A](key: A, quantity: Int) extends CRDTFormat

object ORCart {
  def apply[A]: CRDT[Map[A, Int]] = CRDT(Map.empty[A, Int])

  implicit class ORCartCRDT[A](crdt: CRDT[Map[A, Int]]) {

    def add(key: A, quantity: Int, timestamp: VectorTime)(implicit ops: CRDTNonCommutativePureOp[Map[A, Int]]) = ops.effect(crdt, AddOp(ORCartEntry(key, quantity)), timestamp)
    def remove(key: A, t: VectorTime)(implicit ops: CRDTNonCommutativePureOp[Map[A, Int]]) = ops.effect(crdt, RemoveOp(key), t)
  }

  implicit def ORCartServiceOps[A] = new CRDTNonCommutativePureOp[Map[A, Int]] {
    override def zero: CRDT[Map[A, Int]] = ORCart.apply[A]

    override val obs: Obsolete = (op1, op2) => {
      ((op1.vectorTimestamp, op1.value), (op2.vectorTimestamp, op2.value)) match {
        case ((t1, AddOp(ORCartEntry(k1, _))), (t2, RemoveOp(k2, _))) => (t1 < t2) && (k1 equals k2)
        case ((_, RemoveOp(_, _)), _) => true
        case _ => false
      }
    }

    override def customEval(crdt: CRDT[Map[A, Int]]): Map[A, Int] = crdt.polog.log.foldLeft(Map.empty[A, Int]) {
      case (acc, Versioned(AddOp(ORCartEntry(key: A, quantity)), _, _, _)) => acc.get(key) match {
        case Some(c) => acc + (key -> (c + quantity))
        case None    => acc + (key -> quantity)
      }
      case (acc, Versioned(RemoveOp(_, _), _, _, _)) => acc
    }

    override protected def mergeState(stableState: Map[A, Int], evaluatedState: Map[A, Int]): Map[A, Int] = {
      val keys = stableState.keys ++ evaluatedState.keys
      keys.map(k => {
        val quantity1 = stableState.getOrElse(k, 0)
        val quantity2 = evaluatedState.getOrElse(k, 0)
        (k, quantity1 + quantity2)
      }).toMap
    }
  }
}

/**
 * Replicated [[ORCart]] CRDT service.
 *
 *  - For adding a new `key` of given `quantity` a client should call `add`.
 *  - For incrementing the `quantity` of an existing `key` a client should call `add`.
 *  - For decrementing the `quantity` of an existing `key` a client should call `remove`, followed by `add`
 * (after `remove` successfully completed).
 *  - For removing a `key` a client should call `remove`.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @tparam A [[ORCart]] key type.
 */
class ORCartService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[CRDT[Map[A, Int]], Map[A, Int]])
  extends CRDTService[CRDT[Map[A, Int]], Map[A, Int]] {

  /**
   * Adds the given `quantity` of `key` to the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def add(id: String, key: A, quantity: Int): Future[Map[A, Int]] =
    if (quantity > 0) op(id, AddOp(ORCartEntry(key, quantity))) else Future.failed(new IllegalArgumentException("quantity must be positive"))

  /**
   * Removes the given `key` from the OR-Cart identified by `id` and returns the updated OR-Cart content.
   */
  def remove(id: String, key: A): Future[Map[A, Int]] =
    op(id, RemoveOp(key))

  start()
}
