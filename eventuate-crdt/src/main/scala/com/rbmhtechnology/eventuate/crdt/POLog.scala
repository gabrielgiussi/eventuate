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
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }
import com.rbmhtechnology.eventuate.{ VectorTime, Versioned }

// set o map?
case class POLog(log: Set[Versioned[Operation]] = Set.empty) extends CRDTFormat {

  // TODO implict OBS

  // deberia llamarse desde ops#stable. Alguna manera de restringirlo?
  //p \ {(t,p(t))}
  //def remove(a: Versioned[Operation]): POLog = copy(log - a)

  // estos dos deberian llamarse desde ops#effect
  // puedo poner implicit en obs?
  /**
   * when a new pair (t, o) is delivered to a replica, effect discards from the PO-Log all elements x such that obsolete(x, (t, o)) holds
   */
  private def prune(ops: Set[Versioned[Operation]], r: Versioned[Operation] => Boolean) = ops filter (!r(_)) // TODO review if this is ok

  /**
   * the delivered pair (t, o) is only inserted into the PO-Log if it is not
   * redundant itself, according to the current elements, i.e., if for any current x
   * in the PO-Log obsolete((t, o), x) is false
   */
  def add(op: Versioned[Operation])(implicit red: CausalRedundancy): POLog = {
    //if ((log.isEmpty) || (log.exists { !obs(op, _) })) copy(log + op)
    // TODO check to not add twice the same VectorTime should only be for testing.
    if (!red.r(op, this)) copy(prune(log + op, red.r1(op)))
    else copy(prune(log, red.r0(op)))

    //if ((log.isEmpty) || (log.forall { !obs(op, _) })) copy(log + op) // TODO review if this is ok (forall o exists?)
    //else this
  }

  // TODO Stable can be in VectorTimestamp?
  def stable(stable: VectorTime): (POLog, Seq[Operation]) = {
    val (stableOps, nonStableOps) = log.foldLeft((Seq.empty[Operation], Seq.empty[Versioned[Operation]])) {
      case ((stableOps, nonStableOps), op) if (op.vectorTimestamp <= stable) => (stableOps :+ op.value, nonStableOps)
      case ((stableOps, nonStableOps), op)                                   => (stableOps, nonStableOps :+ op)
    }
    (copy(log = nonStableOps.toSet), stableOps)
  }

}
