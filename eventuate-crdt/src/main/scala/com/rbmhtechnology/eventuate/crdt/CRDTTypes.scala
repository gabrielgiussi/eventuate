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

package object CRDTTypes {

  //type Obsolete[A] = (Versioned[A],Versioned[A]) => Boolean
  // primer parametros = new Pair (t,o)
  type Obsolete = (Versioned[Operation], Versioned[Operation]) => Boolean

  type PruneState[A] = (Operation, A, Obsolete) => A

  type UpdateState[A] = (A, Versioned[Operation]) => A

  type Eval[A] = (POLog, A) => A

  type Operation = Any

  //type State = Any

}
