package com.rbmhtechnology.eventuate.crdt

trait CRDT[A] extends CRDTFormat {

  // TODO cambiar a Set(Versioned[Operation])?
  val polog: POLog

  val state: A

}

trait CRDTHelper[A,T <: CRDT[A]] {
  def copyCRDT(pOLog: POLog, state: A): T
}
