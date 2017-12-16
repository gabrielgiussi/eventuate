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
import com.rbmhtechnology.eventuate.Versioned
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

import scala.concurrent.Future

object Counter {
  def apply[A: Integral]: Counter[A] =
    Counter[A](state = implicitly[Integral[A]].zero)

  /**
   * Adds `delta` (which can also be negative) to the counter `value` and
   * returns an updated counter.
   */
  implicit def merge[A: Integral](s: A, op: Versioned[Operation]) = op.value match {
    case UpdateOp(delta) => implicitly[Integral[A]].plus(s, delta.asInstanceOf[A])
  }

  //implicit def CounterServiceOps[A: Integral] = new CRDTServiceOps[Counter[A], A] {
  implicit def CounterServiceOps[A: Integral] = new CRDTServiceOps[A] {
    override def zero: Counter[A] =
      Counter.apply[A]

    /*
    override val obs: Obsolete = (_, _) => false

    override val pruneState = (op: Operation, state: A, obs: Obsolete) => state

    override val updateState = (state: A, op: Versioned[Operation]) => merge(state, op)
*/
    override def precondition: Boolean =
      false

  }
}

/**
 * Operation-based Counter CRDT.
 *
 * @param state Current counter value.
 * @tparam A Counter value type.
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class Counter[A: Integral](override val polog: POLog = POLog(), override val state: A) extends CRDT[A] with CRDTHelper[A, Counter[A]] {

  override def copyCRDT(polog: POLog, state: A) = copy(polog, state)

  override def eval = {
    polog.log.foldLeft(state)((s: A, op: Versioned[Operation]) => Counter.merge(s, op))
  }

  override val obs: Obsolete = (_, _) => false

  override val pruneState = (op: Operation, state: A, obs: Obsolete) => state

  //override val updateState = (state: A, op: Versioned[Operation]) => Counter.merge(state, op)
}

/**
 * Replicated [[Counter]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log       Event log.
 * @tparam A Counter value type.
 */
class CounterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, integral: Integral[A], val ops: CRDTServiceOps[A])
  //extends CRDTService[Counter[A], A] {
  extends CRDTService[A] {

  /**
   * Adds `delta` (which can also be negative) to the counter identified by `id` and returns the updated counter value.
   */
  def update(id: String, delta: A): Future[A] =
    op(id, UpdateOp(delta))

  start()
}

/**
 * Persistent update operation used for [[Counter]].
 */
case class UpdateOp(delta: Any) extends CRDTFormat
