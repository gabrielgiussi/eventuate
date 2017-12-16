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

import com.rbmhtechnology.eventuate.{ VectorTime, Versioned }
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation, PruneState, UpdateState }

trait CRDTSPI[A] extends CRDTFormat {

  def eval(): A

  final def value(): A = eval()

  protected[crdt] def obs: Obsolete

  def addOp(op: Operation, t: VectorTime, systemTimestamp: Long = 0L, emitterId: String = ""): CRDTSPI[A]

  //protected[crdt] def copyCRDT(pOLog: POLog, state: A): T
}

trait CRDT[A] extends CRDTSPI[A] {

  // TODO implicit
  //protected[crdt] val obs: Obsolete = (_, _) => throw new NotImplementedError()

  // TODO cambiar a Set(Versioned[Operation])? Si hago esto, donde pongo las operaciones que estan en POLog?
  protected[crdt] val polog: POLog // TODO val o def?

  protected[crdt] val state: A

  val pruneState: PruneState[A] = (_, state, _) => state

  lazy val updateState: UpdateState[A] = throw new NotImplementedError()

  final def addOp(op: Operation, t: VectorTime, systemTimestamp: Long = 0L, emitterId: String = ""): CRDT[A] = {
    val versionedOp = Versioned(op, t, systemTimestamp, emitterId)
    val p = polog prune (versionedOp, obs) add (versionedOp, obs)
    val s = pruneState(op, state, obs)
    copyCRDT(p, s)
  }

  // s' = {x ∈ s | ¬ obsolete((⊥, x), (t, o))}
  def effectState(op: Operation) = pruneState(op, state, obs) // TODO use this

  // p' = {x ∈ p | ¬ obsolete(x, (t, o))} ∪ {(t, o) | x ∈ p ⇒ ¬ obsolete((t, o), x)}
  def effectPolog(versionedOp: Versioned[Operation]) = polog prune (versionedOp, obs) add (versionedOp, obs) // TODO use this

  // stable(t, (s, p)) = (s ∪ p(t), p \ {(t, p(t))})
  def stable(t: VectorTime): CRDT[A] = {
    // TODO I must filter here
    polog.op(t) match {
      case Some(op) =>
        val versionedOp = Versioned(op, t)
        copyCRDT(polog remove versionedOp, updateState(state, versionedOp))
      case None => this
    }
  }

  protected def copyCRDT(pOLog: POLog, state: A): CRDT[A]

}

// TODO I guess the point of doing this was to be able to copy and return specific CRDT (e.g. ORSet[A]) instead of CRDT[A] and have to cast
trait CRDTHelper[A, T <: CRDT[A]] {
  protected[crdt] def copyCRDT(pOLog: POLog, state: A): T
}

