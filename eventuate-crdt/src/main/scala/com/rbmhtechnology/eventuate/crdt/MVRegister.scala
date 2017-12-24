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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

import scala.concurrent.Future

object MVRegister {

  implicit class MVRegisterCRDT[A](crdt: CRDT[Set[A]]) {
    def assign(value: A, vectorTime: VectorTime, timestamp: Long = 0L, creator: String = "")(implicit ops: CRDTNonCommutativePureOp[Set[A]]) = ops.effect(crdt, AssignOp(value), vectorTime, timestamp, creator)
  }

  def apply[A]: CRDT[Set[A]] = CRDT(Set.empty[A])

  implicit def MVRegisterServiceOps[A] = new CRDTNonCommutativePureOp[Set[A]] {

    override def precondition: Boolean = false

    override def customEval(crdt: CRDT[Set[A]]): Set[A] = crdt.polog.log.map(_.value.asInstanceOf[AssignOp].value).asInstanceOf[Set[A]]

    override implicit def obs: Obsolete = (op1, op2) => op1.vectorTimestamp < op2.vectorTimestamp

    override def zero: CRDT[Set[A]] = MVRegister.apply[A]

    override protected def mergeState(stableState: Set[A], evaluatedState: Set[A]): Set[A] = stableState ++ evaluatedState
  }

}

/**
 * Replicated [[MVRegister]] CRDT service.
 *
 * @param serviceId Unique id of this service.
 * @param log Event log.
 * @tparam A [[MVRegister]] value type.
 */
class MVRegisterService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[CRDT[Set[A]], Set[A]])
  extends CRDTService[CRDT[Set[A]], Set[A]] {

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
