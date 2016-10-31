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

import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }
import com.rbmhtechnology.eventuate.{ VectorTime, Versioned }

// set o map?
case class POLog(log: Set[Versioned[Operation]] = Set.empty) {

  // puedo asegurar que esta el timestamp y remover el Option?
  // TODO me conviene devolver Versioned[Operation]?
  def op(timestamp: VectorTime): Option[Operation] = log find ((x) => x.vectorTimestamp equiv timestamp) match {
    case Some(Versioned(op, _, _, _)) => Some(op)
    case None                         => None
  }

  // deberia llamarse desde ops#stable. Alguna manera de restringirlo?
  //p \ {(t,p(t))}
  def remove(a: Versioned[Operation]): POLog = copy(log - a)

  // estos dos deberian llamarse desde ops#effect
  // puedo poner implicit en obs?
  def prune(op: Versioned[Operation], obs: Obsolete): POLog = copy(log filter (!obs(_, op)))

  def add(op: Versioned[Operation], obs: Obsolete): POLog = {
    if ((log isEmpty) || (log.exists { !obs(op, _) })) copy(log + op)
    else this

  }

}
