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

object CRDTUtils {

  class EnhancedCRDT[A](crdt: A) {
    def eval[B](implicit ops: CRDTServiceOps[A, B]): B = ops.eval(crdt)

    def value[B](implicit ops: CRDTServiceOps[A, B]): B = eval(ops)
  }

  class EnhancedNonCommutativeCRDT[A](crdt: CRDT[A]) extends EnhancedCRDT[CRDT[A]](crdt) {

    def stable(stableT: VectorTime)(implicit ops: CvRDTPureOp[A, _]) = ops.stable(crdt, stableT)

  }

  implicit class TPSetCRDT[A](crdt: TPSet[A]) extends EnhancedCRDT(crdt) {

    def add(entry: A, t: VectorTime)(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.effect(crdt, AddOp(entry), t)

    def remove(entry: A, t: VectorTime)(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.effect(crdt, RemoveOp(entry), t)

  }

  implicit class AWCartCRDT[A](crdt: SimpleCRDT) extends EnhancedNonCommutativeCRDT[Seq[Operation]](crdt) {

    def add(key: A, quantity: Int, timestamp: VectorTime)(implicit ops: CvRDTPureOpSimple[_]) = ops.effect(crdt, AddOp(AWCartEntry(key, quantity)), timestamp)
    def remove(key: A, t: VectorTime)(implicit ops: CvRDTPureOpSimple[Map[A, Int]]) = ops.effect(crdt, RemoveOp(key), t)
    def clear(vectorTime: VectorTime)(implicit ops: CvRDTPureOpSimple[Map[A, Int]]) = ops.effect(crdt, Clear, vectorTime)
  }

  implicit class AWSetCRDT[A](crdt: AWSet[A]) extends EnhancedNonCommutativeCRDT[Set[A]](crdt) {
    def add(value: A, vectorTime: VectorTime)(implicit ops: CvRDTPureOp[Set[A], Set[A]]) = ops.effect(crdt, AddOp(value), vectorTime)
    def remove(value: A, vectorTime: VectorTime)(implicit ops: CvRDTPureOp[Set[A], Set[A]]) = ops.effect(crdt, RemoveOp(value), vectorTime)
    def clear(vectorTime: VectorTime)(implicit ops: CvRDTPureOp[Set[A], Set[A]]) = ops.effect(crdt, Clear, vectorTime)
  }

  object CounterCRDT {

    implicit class CounterCRDT[A: Integral](crdt: A) extends EnhancedCRDT[A](crdt) {
      def update(delta: A, vt: VectorTime)(implicit ops: CRDTServiceOps[A, A]) = ops.effect(crdt, UpdateOp(delta), vt)
    }

  }

  implicit class LWWRegisterCRDT[A](crdt: SimpleCRDT) extends EnhancedNonCommutativeCRDT[Seq[Operation]](crdt) {
    def assign(value: A, vectorTime: VectorTime, timestamp: Long, creator: String)(implicit ops: CvRDTPureOpSimple[Option[A]]) = ops.effect(crdt, AssignOp(value), vectorTime, timestamp, creator)
    def clear(t: VectorTime)(implicit ops: CvRDTPureOpSimple[Option[A]]) = ops.effect(crdt, Clear, t)
  }

  implicit class MVRegisterCRDT[A](crdt: SimpleCRDT) extends EnhancedNonCommutativeCRDT[Seq[Operation]](crdt) {
    def assign(value: A, vectorTime: VectorTime)(implicit ops: CvRDTPureOpSimple[Set[A]]) = ops.effect(crdt, AssignOp(value), vectorTime)
    def clear(t: VectorTime)(implicit ops: CvRDTPureOpSimple[Set[A]]) = ops.effect(crdt, Clear, t)
  }

}
