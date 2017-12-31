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
import com.rbmhtechnology.eventuate.{ DurableEvent, VectorTime, Versioned }
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

object CRDT {

  implicit class EnhancedCRDT[A](crdt: A) {
    def eval[B](implicit ops: CRDTServiceOps[A, B]): B = ops.eval(crdt)

    def value[B](implicit ops: CRDTServiceOps[A, B]): B = eval(ops)
  }

  def apply[A](state: A): CRDT[A] = CRDT(POLog(), state)
}

case class CRDT[B](polog: POLog, state: B) extends CRDTFormat {

  // TODO this logic can go here or in CRDTPureOp (this has an impact on composability)
  def addOp(op: Operation, timestamp: VectorTime, systemTimestamp: Long = 0L, creator: String = "")(implicit obs: Obsolete) = {
    val versionedOp = Versioned(op, timestamp, systemTimestamp, creator)
    val p = polog prune (versionedOp, obs) add (versionedOp, obs) // TODO make obs implicit param
    // val s = pruneState(op, state, obs) TODO
    copy(polog = p)
  }
}

/*
object CRDTCommutativePureOp {

  implicit class EnhancedCRDT[A](crdt: A) {
    def eval(implicit ops: CRDTCommutativePureOp[A]) = ops.eval(crdt)

    def value(implicit ops: CRDTCommutativePureOp[A]) = eval(ops)
  }
}
*/

trait CRDTNonCommutativePureOp[B] extends CRDTServiceOps[CRDT[B], B] {

  implicit def obs: Obsolete

  def effect(crdt: CRDT[B], op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): CRDT[B] =
    crdt.addOp(op, vt, systemTimestamp, creator)

  protected def mergeState(stableState: B, evaluatedState: B): B

  protected def customEval(crdt: CRDT[B]): B

  final override def eval(crdt: CRDT[B]): B = mergeState(crdt.state, customEval(crdt))

}
