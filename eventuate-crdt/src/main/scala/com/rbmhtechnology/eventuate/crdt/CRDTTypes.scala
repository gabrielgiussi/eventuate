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

  type Redundancy = (Versioned[Operation], POLog) => Boolean

  type Redundancy_ = Versioned[Operation] => Versioned[Operation] => Boolean

  case class CausalRedundancy(r: Redundancy, r0: Redundancy_, r1: Redundancy_) {
    def this(r: Redundancy, r0: Redundancy_) = this(r, r0, r0)
  }

  type SimpleCRDT = CRDT[Seq[Operation]]

}
