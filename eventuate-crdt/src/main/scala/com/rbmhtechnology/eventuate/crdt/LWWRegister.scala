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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

import scala.concurrent.Future
/**
 * Operation-based LWW-Register CRDT with an [[MVRegister]]-based implementation. Instead of returning multiple
 * values in case of concurrent assignments, the last written value is returned. The last written value is
 * determined by comparing the following [[Versioned]] fields in given order:
 *
 *  - `vectorTimestamp`: if causally related, return the value with the higher timestamp, otherwise compare
 *  - `systemTimestamp`: if not equal, return the value with the higher timestamp, otherwise compare
 *  - `emitterId`
 *
 * Note that this relies on synchronized system clocks. [[LWWRegister]] should only be used when the choice of
 * value is not important for concurrent updates occurring within the clock skew.
 *
 * @param mvRegister Initially empty [[MVRegister]].
 * @see [[http://hal.upmc.fr/docs/00/55/55/88/PDF/techreport.pdf A comprehensive study of Convergent and Commutative Replicated Data Types]], specification 9
 */
case class LWWRegister[A](mvRegister: MVRegister[A] = MVRegister.apply[A]) extends CRDTSPI[Option[A]] { // with CRDTHelper[A,LWWRegister[A]]

  /**
   * Assigns a [[Versioned]] value from `v` and `vectorTimestamp` and returns an updated MV-Register.
   *
   * @param v value to assign.
   * @param vectorTimestamp vector timestamp of the value to assign.
   * @param systemTimestamp system timestamp of the value to assign.
   * @param emitterId id of the value emitter.
   */
  def assign(v: A, vectorTimestamp: VectorTime, systemTimestamp: Long, emitterId: String): LWWRegister[A] = {
    copy(mvRegister.assign(v, vectorTimestamp, systemTimestamp, emitterId))
  }

  // FIXME here we are accessing the inner polog of the MVRegister (they were doing the same before with mvRegister.versioned)
  // FIXME, if we want to avoid this we must change the type of MVRegister to Versioned[A]
  // Maybe having MVRegister[Versioned[A]] and MVRegister[A]
  override def eval(): Option[A] =
    mvRegister.polog.log.toVector.sorted(LWWRegister.LWWOrdering[A]).lastOption.map(_.value.asInstanceOf[AssignOp].value.asInstanceOf[A]) // TODO to many cast

  override protected[crdt] def obs: Obsolete = throw new NotImplementedError() // TODO this demonstrates a bad smell in the design

  override def addOp(op: Operation, t: VectorTime, systemTimestamp: Long = 0L, emitterId: String = ""): CRDTSPI[Option[A]] = copy(mvRegister.addOp(op, t, systemTimestamp, emitterId).asInstanceOf[MVRegister[A]])

  //override protected[crdt] def copyCRDT(polog: POLog, state: A): LWWRegister[A] = copy()
}

object LWWRegister {
  def apply[A]: LWWRegister[A] =
    new LWWRegister[A]()

  implicit def LWWRegisterServiceOps[A] = new CRDTServiceOps[Option[A]] {
    override def zero: LWWRegister[A] =
      LWWRegister.apply[A]()

    override def precondition: Boolean =
      false

  }

  implicit def LWWOrdering[A] = new Ordering[Versioned[_]] {
    override def compare(x: Versioned[_], y: Versioned[_]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        x.creator.compareTo(y.creator)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }
}

/**
 * Replicated [[LWWRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[LWWRegister]] value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[Option[A]])
  extends CRDTService[Option[A]] {

  /**
   * Assigns a `value` to the LWW-Register identified by `id` and returns the updated LWW-Register value.
   */
  def assign(id: String, value: A): Future[Option[A]] =
    op(id, AssignOp(value))

  start()
}