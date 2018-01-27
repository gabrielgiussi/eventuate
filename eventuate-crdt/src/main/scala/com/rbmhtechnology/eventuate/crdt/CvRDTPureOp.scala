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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.Versioned
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Redundancy_
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.SimpleCRDT
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable

/**
 * Type-class for pure-op based CRDT.
 *
 * @tparam C CRDT state type
 * @tparam B CRDT value type
 */
trait CvRDTPureOp[C, B] extends CRDTServiceOps[CRDT[C], B] {

  /**
   * The data-type specific relations r,r0 and r1 used for reducing the POLog size via causal redundancy
   */
  implicit def causalRedundancy: CausalRedundancy

  // TODO
  /**
   * This data-type specific method should u
   * The op is always on the causal future of the state
   *
   * @param op the newly delivered operation
   * @param state the current CRDT state
   * @return the updated state
   */
  protected def updateState(op: Operation, redundant: Boolean, state: C): C

  /**
   * Adds the operation to the POLog using the causal redundancy relations and also updates the state
   * with the new operation.
   *
   * @param crdt
   * @param op
   * @param vt              operation's timestamp
   * @param systemTimestamp operation's metadata
   * @param creator         operation's metadata
   * @return
   */
  final def effect(crdt: CRDT[C], op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): CRDT[C] = {
    val versionedOp = Versioned(op, vt, systemTimestamp, creator)
    val (updatedPolog, redundant) = crdt.polog.add(versionedOp)
    val updatedState = updateState(op, redundant, crdt.state)
    crdt.copy(updatedPolog, updatedState)
  }

  override def eval(crdt: CRDT[C]): B

  /**
   * "This function takes a stable
   * timestamp t (fed by the TCSB middleware) and the full PO-Log s as input, and returns
   * a new PO-Log (i.e., a map), possibly discarding a set of operations at once."
   *
   * @param polog
   * @param stable
   * @return
   */
  protected def stabilize(polog: POLog, stable: TCStable): POLog = polog

  /**
   * Updates the current [[CRDT.state]] with the sequence of stable operations that were removed
   * from the POLog on [[POLog.stable]]
   * The implementation will vary depending on the CRDT's state type.
   *
   * @param state the current CRDT state
   * @param stableOps the sequence of stable operations that were removed from the POLog
   * @return the updated state
   */
  protected def stabilizeState(state: C, stableOps: Seq[Operation]): C

  /**
   * "The stable
   * handler, on the other hand, invokes stabilize and then strips the timestamp (if the operation
   * has not been discarded by stabilize), by replacing a (t', o') pair that is present in the returned
   * PO-Log by (⊥, o')"
   * The actual implementation doesn't replace the timestamp with ⊥, instead it calls [[stabilizeState]] with
   * the current [[CRDT.state]] and the sequence of stable operations discarded from the [[POLog]].
   *
   * @param crdt a crdt
   * @param stable a stable VectorTime fed by the middleware
   * @return a possible optimized crdt after discarding operations and removing timestamps
   */
  override def stable(crdt: CRDT[C], stable: TCStable) = {
    val (stabilizedPOLog, stableOps) = stabilize(crdt.polog, stable) stable (stable)
    val stabilizedState = stabilizeState(crdt.state, stableOps)
    crdt.copy(stabilizedPOLog, stabilizedState)
  }

  /**
   * A pure-op based doesn't check preconditions because the framework just
   * returns the unmodified operation on prepare
   */
  override def precondition: Boolean = false

}

/**
 * A base trait for pure-op based CRDTs wich state is a sequence of stable operations
 *
 * @tparam B CRDT value type
 */
trait CvRDTPureOpSimple[B] extends CvRDTPureOp[Seq[Operation], B] {

  final override def zero: SimpleCRDT = CRDT.zero

  /**
   * Adds the stable operations to the state of the CRDT
   *
   * @param state     the current state of the CRDT
   * @param stableOps the causaly stable operations
   * @return the updated state
   */
  override protected def stabilizeState(state: Seq[Operation], stableOps: Seq[Operation]): Seq[Operation] = state ++ stableOps

  /**
   * To simplify the eval method, it asociates the [[VectorTime.Zero]] to each stable operation and calls [[customEval]]
   * over the full sequence of operations, the ones from the POLog and the ones from the state.
   * Note: this has to be a sequence instead of a Set because now the VectorTime is not unique
   */
  override final def eval(crdt: SimpleCRDT): B = {
    val stableOps = crdt.state.map(op => Versioned(op, VectorTime.Zero))
    customEval(stableOps ++ crdt.polog.log)
  }

  // TODO
  /**
   * This method
   *
   * @param ops the full sequence of ops, stable (with the [[VectorTime.Zero]]) and non-stable
   * @return the value of the crdt
   */
  protected def customEval(ops: Seq[Versioned[Operation]]): B

  // TODO
  def optimizedUpdateState: PartialFunction[(Operation, Seq[Operation]), Seq[Operation]] = PartialFunction.empty

  // TODO
  private def defaultUpdateState(redundancy: Redundancy_)(stateAndOp: (Operation, Seq[Operation])) = {
    val (op, state) = stateAndOp
    val one = VectorTime.Zero.increment("a")
    val redundant = redundancy(Versioned(op, one))
    state.map(Versioned(_, VectorTime.Zero)) filterNot redundant map (_.value)
  }

  override final protected def updateState(op: Operation, redundant: Boolean, state: Seq[Operation]): Seq[Operation] =
    optimizedUpdateState.applyOrElse((op, state), defaultUpdateState(causalRedundancy.redundancyFilter(redundant)))

}
