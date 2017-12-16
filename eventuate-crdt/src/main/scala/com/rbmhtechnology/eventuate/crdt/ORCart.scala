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

/**
 * Operation-based OR-Cart CRDT with an [[ORSet]]-based implementation. [[Versioned]] entry values are of
 * type [[ORCartEntry]] and are uniquely identified with vector timestamps.
 *
 * @param orSet Backing [[ORSet]].
 * @tparam A Key type.
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 21.
 * @see [[ORCartEntry]]
 */
case class ORCart[A](orSet: ORSet[Versioned[ORCartEntry[A]]] = ORSet[Versioned[ORCartEntry[A]]]()) extends CRDTSPI[Map[A, Int]] {
  /**
   * Returns the `quantity`s of the contained `key`s, reducing multiple [[ORCartEntry]]s with the same `key`
   * by adding their `quantity`s.
   */
  /*
  def value: Map[A, Int] = {
    orSet.versionedEntries.foldLeft(Map.empty[A, Int]) {
      case (acc, Versioned(ORCartEntry(key, quantity), _, _, _)) => acc.get(key) match {
        case Some(c) => acc + (key -> (c + quantity))
        case None => acc + (key -> quantity)
      }
    }
  }*/

  /**
   * Adds the given `quantity` of `key`, uniquely identified by `timestamp`, and returns an updated `ORCart`.
   */
  def add(key: A, quantity: Int, timestamp: VectorTime): ORCart[A] =
    copy(orSet = orSet.add(Versioned(ORCartEntry(key, quantity), timestamp), timestamp))

  /**
   * Collects all timestamps of given `key`.
   */
  //def prepareRemove(key: A): Set[VectorTime] =
  //  orSet.versionedEntries.collect { case Versioned(ORCartEntry(`key`, _), timestamp, _, _) => timestamp }

  /**
   * Removes all [[ORCartEntry]]s identified by given `timestamps` and returns an updated `ORCart`.
   */
  //def remove(timestamps: Set[VectorTime]): ORCart[A] =
  //  copy(orSet = orSet.remove(timestamps))
  // It should be better if I remove by key but doesn't work
  def remove(key: A, quantity: Int, t: VectorTime) = copy(orSet.remove(Versioned(ORCartEntry(key, quantity), t), t))
  //remove byKeyorSet.eval().find(_.value.key equals (key)).fold(this)(e => copy(orSet.remove(e, t)))

  override protected[crdt] def obs: Obsolete = throw new NotImplementedError() // TODO this demonstrates a bad smell in the design

  override def addOp(op: Operation, t: VectorTime, systemTimestamp: Long, emitterId: String): CRDTSPI[Map[A, Int]] =
    copy(orSet.addOp(op, t, systemTimestamp, emitterId).asInstanceOf[ORSet[Versioned[ORCartEntry[A]]]])

  override def eval(): Map[A, Int] = {
    println(orSet.polog)
    orSet.polog.log.foldLeft(Map.empty[A, Int]) {
      case (acc, Versioned(AddOp(ORCartEntry(key: A, quantity)), _, _, _)) => acc.get(key) match {
        case Some(c) => acc + (key -> (c + quantity))
        case None    => acc + (key -> quantity)
      }
    }
  }
}

object ORCart {
  def apply[A]: ORCart[A] =
    new ORCart[A]()

  implicit def ORCartServiceOps[A] = new CRDTServiceOps[Map[A, Int]] {
    override def zero: ORCart[A] =
      ORCart.apply[A]

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
class ORCartService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[Map[A, Int]])
  extends CRDTService[Map[A, Int]] {

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
