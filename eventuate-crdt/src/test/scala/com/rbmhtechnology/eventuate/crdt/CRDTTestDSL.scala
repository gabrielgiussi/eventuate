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

import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.AWSet.AWSet
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.SimpleCRDT
import com.rbmhtechnology.eventuate.crdt.TPSet.TPSet

import scala.collection.immutable.Set

object CRDTTestDSL {

  case class StableVectorTime(vt: VectorTime)

  class EnhancedCRDT[A](crdt: A) {
    def eval[B](implicit ops: CRDTServiceOps[A, B]): B = ops.eval(crdt)

    def value[B](implicit ops: CRDTServiceOps[A, B]): B = eval(ops)
  }

  class EnhancedCvRDT[A](crdt: CRDT[A]) extends EnhancedCRDT[CRDT[A]](crdt) {

    def stable(stable: StableVectorTime)(implicit ops: CvRDTPureOp[A, _]) = ops.stable(crdt, stable.vt)

  }

  implicit class TPSetCRDT[A](crdt: TPSet[A]) extends EnhancedCRDT(crdt) {

    def add(entry: A, t: VectorTime)(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.effect(crdt, AddOp(entry), t)

    def remove(entry: A, t: VectorTime)(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.effect(crdt, RemoveOp(entry), t)

  }

  implicit class AWCartCRDT[A](crdt: SimpleCRDT) extends EnhancedCvRDT[Seq[Operation]](crdt) {

    def add(key: A, quantity: Int, timestamp: VectorTime)(implicit ops: CvRDTPureOpSimple[_]) = ops.effect(crdt, AddOp(AWCartEntry(key, quantity)), timestamp)
    def remove(key: A, t: VectorTime)(implicit ops: CvRDTPureOpSimple[Map[A, Int]]) = ops.effect(crdt, RemoveOp(key), t)
    def clear(vectorTime: VectorTime)(implicit ops: CvRDTPureOpSimple[Map[A, Int]]) = ops.effect(crdt, Clear, vectorTime)
  }

  implicit class AWSetCRDT[A](crdt: AWSet[A]) extends EnhancedCvRDT[Set[A]](crdt) {
    def add(value: A, vectorTime: VectorTime)(implicit ops: CvRDTPureOp[Set[A], Set[A]]) = ops.effect(crdt, AddOp(value), vectorTime)
    def remove(value: A, vectorTime: VectorTime)(implicit ops: CvRDTPureOp[Set[A], Set[A]]) = ops.effect(crdt, RemoveOp(value), vectorTime)
    def clear(vectorTime: VectorTime)(implicit ops: CvRDTPureOp[Set[A], Set[A]]) = ops.effect(crdt, Clear, vectorTime)
  }

  implicit class CounterCRDT[A: Integral](crdt: A) extends EnhancedCRDT[A](crdt) {
    def update(delta: A, vt: VectorTime)(implicit ops: CRDTServiceOps[A, A]) = ops.effect(crdt, UpdateOp(delta), vt)
  }

  implicit class LWWRegisterCRDT[A](crdt: SimpleCRDT) extends EnhancedCvRDT[Seq[Operation]](crdt) {
    def assign(value: A, vectorTime: VectorTime, timestamp: Long, creator: String)(implicit ops: CvRDTPureOpSimple[Option[A]]) = ops.effect(crdt, AssignOp(value), vectorTime, timestamp, creator)
    def clear(t: VectorTime)(implicit ops: CvRDTPureOpSimple[Option[A]]) = ops.effect(crdt, Clear, t)
  }

  implicit class MVRegisterCRDT[A](crdt: SimpleCRDT) extends EnhancedCvRDT[Seq[Operation]](crdt) {
    def assign(value: A, vectorTime: VectorTime)(implicit ops: CvRDTPureOpSimple[Set[A]]) = ops.effect(crdt, AssignOp(value), vectorTime)
    def clear(t: VectorTime)(implicit ops: CvRDTPureOpSimple[Set[A]]) = ops.effect(crdt, Clear, t)
  }

  trait VectorTimeControl {
    var emitted = Set.empty[VectorTime]
    var stable = Set.empty[VectorTime]

    private def _vt(t1: Long, t2: Long) = VectorTime("p1" -> t1, "p2" -> t2)

    def vt(t1: Long, t2: Long): VectorTime = {
      val newVT = _vt(t1, t2)
      if (emitted.contains(newVT)) throw new RuntimeException(s"you are trying to add $newVT twice")
      stable.find(_ || newVT).foreach(st => throw new RuntimeException(s"you are trying to add a $newVT but is concurrent to the stable $st"))
      emitted += newVT
      newVT
    }

    def stableVT(t1: Long, t2: Long): StableVectorTime = {
      val newVT = _vt(t1, t2)
      stable += newVT
      StableVectorTime(newVT)
    }

    def clearVTHistory() = {
      stable = Set.empty
      emitted = Set.empty
    }
  }

}
