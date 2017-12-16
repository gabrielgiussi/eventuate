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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Obsolete

import scala.concurrent.Future
/**
 * Operation-based MV-Register CRDT. Has several [[Versioned]] values assigned in case of concurrent assignments,
 * otherwise, a single [[Versioned]] value. Concurrent assignments can be reduced to a single assignment by
 * assigning a [[Versioned]] value with a vector timestamp that is greater than those of the currently assigned
 * [[Versioned]] values.
 *
 * @param versioned Assigned values. Initially empty.
 * @tparam A Assigned value type.
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]]
 */
case class MVRegister[A](override val polog: POLog = POLog(), override val state: Set[A] = Set.empty[A]) extends CRDT[Set[A]] with CRDTHelper[Set[A], MVRegister[A]] {

  /**
   * Assigns a [[Versioned]] value from `v` and `vectorTimestamp` and returns an updated MV-Register.
   *
   * @param v value to assign.
   * @param vectorTimestamp vector timestamp of the value to assign.
   * @param systemTimestamp system timestamp of the value to assign.
   * @param emitterId id of the value emitter.
   */
  def assign(v: A, vectorTimestamp: VectorTime, systemTimestamp: Long = 0L, emitterId: String = ""): MVRegister[A] = {
    addOp(AssignOp(v), vectorTimestamp, systemTimestamp, emitterId).asInstanceOf[MVRegister[A]]
  }

  //def clear(t: VectorTime): MVRegister[A] = addOp(Clear(),t).asInstanceOf[MVRegister[A]]

  // the effect of a write in making all writes in its causal past obsolete, regardless of value written
  // obsolete((t, [wr, v]), (t', [wr, v'])) = t < t'
  override protected[crdt] val obs: Obsolete = (op1, op2) => op1.vectorTimestamp < op2.vectorTimestamp

  // eval(rd, s) = {v | (t, [wr, v]) ∈ s}
  override def eval(): Set[A] = polog.log.map(_.value.asInstanceOf[AssignOp].value).asInstanceOf[Set[A]]

  override def copyCRDT(polog: POLog, state: Set[A]): MVRegister[A] = copy(polog, state)
}

object MVRegister {
  def apply[A]: MVRegister[A] =
    new MVRegister[A]()

  implicit def MVRegisterServiceOps[A] = new CRDTServiceOps[Set[A]] {
    override def zero: MVRegister[A] =
      MVRegister.apply[A]

    override def precondition: Boolean =
      false

  }
}

/**
 * Replicated [[MVRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[MVRegister]] value type.
 */
class MVRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[Set[A]])
  extends CRDTService[Set[A]] {

  /**
   * Assigns a `value` to the MV-Register identified by `id` and returns the updated MV-Register value.
   */
  def assign(id: String, value: A): Future[Set[A]] =
    op(id, AssignOp(value))

  start()
}

/**
 * Persistent assign operation used for [[MVRegister]] and [[LWWRegister]].
 */
case class AssignOp(value: Any) extends CRDTFormat

//case class Clear() extends CRDTFormat