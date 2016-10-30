package com.rbmhtechnology.eventuate.crdt

import com.rbmhtechnology.eventuate.crdt.CRDTTypes.{ Obsolete, Operation }
import com.rbmhtechnology.eventuate.{ VectorTime, Versioned }

// set o map?
case class POLog(log: Set[Versioned[Operation]] = Set.empty) {

  // puedo asegurar que esta el timestamp y remover el Option?
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
