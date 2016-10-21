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

import akka.actor._
import akka.serialization.Serializer

import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTFormats._
import com.rbmhtechnology.eventuate.crdt.CRDTService._
import com.rbmhtechnology.eventuate.serializer.CommonSerializer

import scala.collection.JavaConverters._

class CRDTSerializer(system: ExtendedActorSystem) extends Serializer {
  val commonSerializer = new CommonSerializer(system)

  private val MatchClass = classOf[Match[_]]
  private val ValueUpdatedClass = classOf[ValueUpdated]
  //private val UpdatedOpClass = classOf[UpdateOp]
  private val AddOpClass = classOf[AddOp]
  private val RemoveOpClass = classOf[RemoveOp]

  override def identifier: Int = 22567
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case r: Match[_] =>
      matchFormatBuilder(r).build().toByteArray
    case v: ValueUpdated =>
      valueUpdatedFormat(v).build().toByteArray
    case o: AddOp =>
      addOpFormatBuilder(o).build().toByteArray
    case o: RemoveOp =>
      removeOpFormatBuilder(o).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case MatchClass =>
        aMatch(MatchFormat.parseFrom(bytes))
      case ValueUpdatedClass =>
        valueUpdated(ValueUpdatedFormat.parseFrom(bytes))
      case AddOpClass =>
        addOp(AddOpFormat.parseFrom(bytes))
      case RemoveOpClass =>
        removeOp(RemoveOpFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def matchFormatBuilder(aMatch: Match[_]): MatchFormat.Builder = {
    val builder = MatchFormat.newBuilder

    aMatch.versionedEntries.foreach { ve =>
      builder.addVersionedEntries(commonSerializer.versionedFormatBuilder(ve))
    }

    builder
  }

  private def valueUpdatedFormat(valueUpdated: ValueUpdated): ValueUpdatedFormat.Builder =
    ValueUpdatedFormat.newBuilder.setOperation(commonSerializer.payloadFormatBuilder(valueUpdated.operation.asInstanceOf[AnyRef]))

  private def addOpFormatBuilder(op: AddOp): AddOpFormat.Builder =
    AddOpFormat.newBuilder.setEntry(commonSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

  private def removeOpFormatBuilder(op: RemoveOp): RemoveOpFormat.Builder = {
    val builder = RemoveOpFormat.newBuilder

    builder.setEntry(commonSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

    op.timestamps.foreach { timestamp =>
      builder.addTimestamps(commonSerializer.vectorTimeFormatBuilder(timestamp))
    }

    builder
  }

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def aMatch(matchFormat: MatchFormat): Match[Any] = {
    val ves = matchFormat.getVersionedEntriesList.iterator.asScala.foldLeft(Set.empty[Versioned[Any]]) {
      case (acc, ve) => acc + commonSerializer.versioned(ve)
    }

    Match(ves)
  }

  private def valueUpdated(valueUpdatedFormat: ValueUpdatedFormat): ValueUpdated =
    ValueUpdated(commonSerializer.payload(valueUpdatedFormat.getOperation))

  private def addOp(opFormat: AddOpFormat): AddOp =
    AddOp(commonSerializer.payload(opFormat.getEntry))

  private def removeOp(opFormat: RemoveOpFormat): RemoveOp = {
    val timestamps = opFormat.getTimestampsList.iterator().asScala.foldLeft(Set.empty[VectorTime]) {
      case (result, timestampFormat) => result + commonSerializer.vectorTime(timestampFormat)
    }

    RemoveOp(commonSerializer.payload(opFormat.getEntry), timestamps)
  }
}
