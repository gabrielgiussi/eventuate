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
import com.rbmhtechnology.eventuate.crdt.CRDT.EnhancedNonCommutativeCRDT
import com.rbmhtechnology.eventuate.crdt.CRDT.SimpleCRDT
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.CausalRedundancy
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.R
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.R_
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

import scala.concurrent.Future

object LWWRegister {

  def apply(): SimpleCRDT = LWWRegisterServiceOps.zero

  implicit class LWWRegisterCRDT[A](crdt: SimpleCRDT) extends EnhancedNonCommutativeCRDT(crdt) {
    def assign(value: A, vectorTime: VectorTime, timestamp: Long, creator: String)(implicit ops: CRDTNonCommutativePureOpSimple[_]) = ops.effect(crdt, AssignOp(value), vectorTime, timestamp, creator)
  }

  implicit def LWWOrdering[A] = new Ordering[Versioned[_]] {
    override def compare(x: Versioned[_], y: Versioned[_]): Int =
      if (x.systemTimestamp == y.systemTimestamp)
        x.creator.compareTo(y.creator)
      else
        x.systemTimestamp.compareTo(y.systemTimestamp)
  }

  implicit def LWWRegisterServiceOps[A] = new CRDTNonCommutativePureOpSimple[Option[A]] {

    override def precondition: Boolean =
      false

    override def customEval(ops: Seq[Versioned[Operation]]): Option[A] = ops.headOption.map(_.value.asInstanceOf[AssignOp].value.asInstanceOf[A])

    val r: R = (op, _) => op.value.isInstanceOf[Clear.type]

    val r0: R_ = newOp => op => {
      if (op.vectorTimestamp || newOp.vectorTimestamp) LWWRegister.LWWOrdering[A].lt(op, newOp)
      else op.vectorTimestamp < newOp.vectorTimestamp
    }

    override implicit val causalRedundancy: CausalRedundancy = new CausalRedundancy(r, r0)
  }

}

/**
 * Replicated [[LWWRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[LWWRegister]] value type.
 */
class LWWRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTNonCommutativePureOpSimple[Option[A]])
  extends CRDTService[SimpleCRDT, Option[A]] {

  /**
   * Assigns a `value` to the LWW-Register identified by `id` and returns the updated LWW-Register value.
   */
  def assign(id: String, value: A): Future[Option[A]] =
    op(id, AssignOp(value))

  def clear(id: String): Future[Option[A]] =
    op(id, Clear) // TODO untested!

  start()
}
