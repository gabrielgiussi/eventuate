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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Eval, Obsolete, Operation }
import com.rbmhtechnology.eventuate.{ VectorTime, Versioned }

import scala.concurrent.Future

/**
 * Operation-based Counter CRDT.
 *
 * @param value Current counter value.
 * @tparam A Counter value type.
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class Counter[A: Integral](polog: POLog = POLog(), value: A) {
  /**
   * Adds `delta` (which can also be negative) to the counter `value` and
   * returns an updated counter.
   */
  //def update(delta: A): Counter[A] = copy(value = implicitly[Integral[A]].plus(value, delta))
}

object Counter {
  def apply[A: Integral]: Counter[A] =
    Counter[A](value = implicitly[Integral[A]].zero)

  implicit def CounterServiceOps[A: Integral] = new CRDTServiceOps[Counter[A], A] {
    override def zero: Counter[A] =
      Counter.apply[A]

    val a = (s: A, op: Versioned[Operation]) => op.value match {
      case UpdateOp(delta) => implicitly[Integral[A]].plus(s, delta.asInstanceOf[A])
    }

    val eval: Eval[A] = (polog: POLog, state: A) => polog.log.foldLeft(state)((s: A, op: Versioned[Operation]) => a(s, op))

    val obs = (op1: Versioned[Operation], op2: Versioned[Operation]) => false

    val pruneState = (s: A, o: Obsolete) => s

    val updateState = (s: A, op: Versioned[Operation]) => s

    override def value(crdt: Counter[A]): A = eval(crdt.polog, crdt.value)

    override def precondition: Boolean =
      false

    def effect(crdt: Counter[A], op: Operation, t: VectorTime): Counter[A] = {
      val newPolog = crdt.polog prune (Versioned(op, t), obs) add (Versioned(op, t), obs)
      crdt.copy(newPolog, pruneState(crdt.value, obs))
    }

    override def stable(crdt: Counter[A], t: VectorTime): Counter[A] = {
      crdt.polog.op(t) match {
        case Some(op) => crdt.copy(crdt.polog remove (Versioned(op, t)), updateState(crdt.value, Versioned(op, t)))
        case None     => crdt
      }

    }
  }
}

/**
 * Replicated [[Counter]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A Counter value type.
 */
class CounterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, integral: Integral[A], val ops: CRDTServiceOps[Counter[A], A])
  extends CRDTService[Counter[A], A] {

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
