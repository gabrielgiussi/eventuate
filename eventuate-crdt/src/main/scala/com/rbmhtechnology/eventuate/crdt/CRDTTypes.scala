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

import com.rbmhtechnology.eventuate.Versioned

object CRDTTypes {

  //type PruneState[A] = (Operation, A, Obsolete) => A

  //type UpdateState[A] = (A, Versioned[Operation]) => A

  type Operation = Any

  /**
   * Returns true if the operation is itself redundant or it is redundant by the operations
   * currently on the POLog.
   * "In most cases this can be decided by looking only at the delivered operation itself"
   */
  type Redundancy = (Versioned[Operation], POLog) => Boolean

  /**
   * Returns true if the second operation (the operation currently in the POLog is made redundant by
   * the first one (the new delivered one)
   */
  type Redundancy_ = Versioned[Operation] => Versioned[Operation] => Boolean

  /**
   *
   * @param r  "defines whether the delivered operation is itself redundant
   *           and does not need to be added itself to the PO-Log."
   * @param r0 "is used when the new delivered operation is discarded being redundant."
   * @param r1 "is used if the new delivered operation is added to the PO-Log."
   */
  case class CausalRedundancy(r: Redundancy, r0: Redundancy_, r1: Redundancy_) {

    /**
     * For most data types, the relations r0 and r1 will be equal
     */
    def this(r: Redundancy, r0: Redundancy_) = this(r, r0, r0)
  }

  /**
   * A CRDT wich state is a sequence of stable operations (that's why they doesn't contain a related VectorTime)
   * This is the CRDT to use if it is not needed to define a specific state type (like a HashSet for AWSet)
   */
  type SimpleCRDT = CRDT[Seq[Operation]]

}
