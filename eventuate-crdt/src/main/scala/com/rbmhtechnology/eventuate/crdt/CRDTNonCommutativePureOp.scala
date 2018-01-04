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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.CausalRedundancy
import com.rbmhtechnology.eventuate.{ DurableEvent, VectorTime, Versioned }
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }

object CRDT {

  type SimpleCRDT = CRDT[Seq[Operation]]

  implicit class EnhancedCRDT[A](crdt: A) {
    def eval[B](implicit ops: CRDTServiceOps[A, B]): B = ops.eval(crdt)

    def value[B](implicit ops: CRDTServiceOps[A, B]): B = eval(ops)
  }

  implicit class EnhancedNonCommutativeCRDT[A](crdt: CRDT[A]) extends EnhancedCRDT(crdt) {

    def stable(stableT: VectorTime)(implicit ops: CRDTNonCommutativePureOp[_, A]) = ops.stable(crdt, stableT)

  }

  def apply[A](state: A): CRDT[A] = CRDT(POLog(), state)

  def zero: CRDT[Seq[Operation]] = CRDT(POLog(), Seq.empty)
}

case class CRDT[B](polog: POLog, state: B) extends CRDTFormat

// FIXME switch types
trait CRDTNonCommutativePureOp[B, C] extends CRDTServiceOps[CRDT[C], B] {

  implicit def causalRedundancy: CausalRedundancy

  def updateState(op: Operation, state: C): C

  def effect(crdt: CRDT[C], op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): CRDT[C] = {
    val versionedOp = Versioned(op, vt, systemTimestamp, creator)
    val updatedPolog = crdt.polog.add(versionedOp)
    val updatedState = updateState(op, crdt.state)
    crdt.copy(updatedPolog, updatedState)
  }

  override def eval(crdt: CRDT[C]): B

  protected def stabilize(polog: POLog, stable: VectorTime): POLog = polog

  protected def stabilizeState(state: C, stableOps: Seq[Operation]): C

  protected[crdt] def stable(crdt: CRDT[C], stable: VectorTime) = {
    val (stabilizedPOLog, stableOps) = stabilize(crdt.polog, stable) stable (stable)
    val stabilizedState = stabilizeState(crdt.state, stableOps)
    crdt.copy(stabilizedPOLog, stabilizedState)
  }

}

trait CRDTNonCommutativePureOpSimple[B] extends CRDTNonCommutativePureOp[B, Seq[Operation]] {

  final override def zero: CRDT[Seq[Operation]] = CRDT.zero

  override protected def stabilizeState(state: Seq[Operation], stableOps: Seq[Operation]): Seq[Operation] = state ++ stableOps

  override def eval(crdt: CRDT[Seq[Operation]]): B = {
    val stableOps = crdt.state.map(op => Versioned(op, VectorTime.Zero))
    customEval(stableOps ++ crdt.polog.log)
  }

  protected def customEval(ops: Seq[Versioned[Operation]]): B

}
