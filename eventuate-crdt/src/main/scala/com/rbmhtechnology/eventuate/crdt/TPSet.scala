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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.crdt.TPSet.TPSet

import scala.collection.immutable.Set
import scala.concurrent.Future

object TPSet {

  type TPSet[A] = (Set[A], Set[A])

  def apply[A]: TPSet[A] = (Set.empty[A], Set.empty[A])

  implicit class TPSetCRDT[A](crdt: TPSet[A]) {

    // TODO can reuse the one of Counter or even use only one for CRDTNonCommutatives to?
    def eval()(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.eval(crdt)

    def add(entry: A, t: VectorTime)(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.effect(crdt, AddOp(entry), t)

    def remove(entry: A, t: VectorTime)(implicit ops: CRDTServiceOps[TPSet[A], Set[A]]) = ops.effect(crdt, RemoveOp(entry), t)

  }

  implicit def TPSetServiceOps[A] = new CRDTServiceOps[TPSet[A], Set[A]] {

    override def zero: TPSet[A] = TPSet.apply[A]

    override def eval(crdt: TPSet[A]): Set[A] = crdt._1

    override def effect(crdt: TPSet[A], op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): (Set[A], Set[A]) =
      (op, crdt) match {
        case (AddOp(e: A), (added, removed)) if (!removed.contains(e)) => (added + e, removed)
        case (RemoveOp(e: A, _), (added, removed)) => (added - e, removed + e)
        case (_, crdt) => crdt
      }
  }

}

class TPSetService[A](val serviceId: String, val log: ActorRef)(implicit val system: ActorSystem, val ops: CRDTServiceOps[TPSet[A], Set[A]]) extends CRDTService[TPSet[A], Set[A]] {

  def add(id: String, entry: A): Future[Set[A]] =
    op(id, AddOp(entry))

  /**
   * Removes `entry` from the OR-Set identified by `id` and returns the updated entry set.
   */
  def remove(id: String, entry: A): Future[Set[A]] =
    op(id, RemoveOp(entry))

  start()
}
