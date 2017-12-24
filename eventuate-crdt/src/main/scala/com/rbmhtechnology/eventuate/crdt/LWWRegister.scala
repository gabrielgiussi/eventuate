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

object LWWRegister {

  def apply[A]: CRDT[Option[A]] = CRDT(None)

  implicit class LWWRegisterCRDT[A](crdt: CRDT[Option[A]]) {
    def assign(value: A, vectorTime: VectorTime, timestamp: Long, creator: String)(implicit ops: CRDTNonCommutativePureOp[Option[A]]) = ops.effect(crdt, AssignOp(value), vectorTime, timestamp, creator)
  }

  implicit def LWWOrdering[A] = new Ordering[Versioned[_]] {
    override def compare(x: Versioned[_], y: Versioned[_]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        x.creator.compareTo(y.creator)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }

  implicit def LWWRegisterServiceOps[A] = new CRDTNonCommutativePureOp[Option[A]] {

    override def precondition: Boolean =
      false

    override implicit def obs: Obsolete = (op1, op2) => {
      if (op1.vectorTimestamp || op2.vectorTimestamp) LWWRegister.LWWOrdering[A].lt(op1, op2)
      else op1.vectorTimestamp < op2.vectorTimestamp
    }

    override def zero: CRDT[Option[A]] = LWWRegister.apply[A]

    override def customEval(crdt: CRDT[Option[A]]): Option[A] = crdt.polog.log.headOption.map(_.value.asInstanceOf[AssignOp].value.asInstanceOf[A])

    override protected def mergeState(stableState: Option[A], evaluatedState: Option[A]): Option[A] = // TODO
      if (stableState.isEmpty) evaluatedState
      else stableState
  }

}

/**
 * Replicated [[LWWRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[LWWRegister]] value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[CRDT[Option[A]], Option[A]])
  extends CRDTService[CRDT[Option[A]], Option[A]] {

  /**
   * Assigns a `value` to the LWW-Register identified by `id` and returns the updated LWW-Register value.
   */
  def assign(id: String, value: A): Future[Option[A]] =
    op(id, AssignOp(value))

  start()
}
