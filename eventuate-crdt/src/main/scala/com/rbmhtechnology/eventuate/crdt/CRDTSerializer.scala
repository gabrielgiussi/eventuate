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

import java.io.{ IOException, _ }
import java.util.logging.Logger

import akka.actor._
import akka.serialization.Serializer
import com.google.protobuf.ByteString
import com.rbmhtechnology.eventuate.crdt.CRDTTypes.Operation
import com.rbmhtechnology.eventuate.{ VectorTime, Versioned }
import com.rbmhtechnology.eventuate.crdt.CRDTFormats._
import com.rbmhtechnology.eventuate.crdt.CRDTService._
import com.rbmhtechnology.eventuate.serializer.CommonSerializer

import scala.collection.JavaConverters._

class CRDTSerializer(system: ExtendedActorSystem) extends Serializer {
  val commonSerializer = new CommonSerializer(system)

  import commonSerializer.payloadSerializer

  private val ORSetClass = classOf[ORSet[_]]
  private val UpdatedOpClass = classOf[UpdateOp]
  private val ValueUpdatedClass = classOf[ValueUpdated]
  private val AddOpClass = classOf[AddOp]
  private val RemoveOpClass = classOf[RemoveOp]
  private val StableClass = classOf[Stable]

  override def identifier: Int = 22567

  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case s: Stable => stableFormatBuilder(s).build().toByteArray
    case s: ORSet[_] =>
      orSetFormatBuilder(s).build().toByteArray
    case v: ValueUpdated =>
      valueUpdatedFormat(v).build().toByteArray
    case o: UpdateOp =>
      updateOpFormatBuilder(o).build().toByteArray
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
      case ORSetClass =>
        orSet(ORSetFormat.parseFrom(bytes))
      case StableClass => stable(StableFormat.parseFrom(bytes))
      case ValueUpdatedClass =>
        valueUpdated(ValueUpdatedFormat.parseFrom(bytes))
      case UpdatedOpClass =>
        updateOp(UpdateOpFormat.parseFrom(bytes))
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

  private def pologFormatBuilder(polog: POLog): POLogFormat.Builder = {
    val builder = POLogFormat.newBuilder

    polog.log.foreach { ve => builder.addVersionedEntries(commonSerializer.versionedFormatBuilder(ve)) }

    builder
  }

  private def orSetFormatBuilder(orSet: ORSet[_]): ORSetFormat.Builder = {
    val builder = ORSetFormat.newBuilder
    builder.setPolog(pologFormatBuilder(orSet.polog))
    orSet.state.foreach(x => {
      val bos = new ByteArrayOutputStream()
      try {
        val out = new ObjectOutputStream(bos);
        out.writeObject(x);
        out.flush();
        builder.addState(ByteString.copyFrom(bos.toByteArray()))
      } finally {
        try {
          bos.close();
        } catch {
          case ex: IOException => println("IOException")
        }
      }
    }
    )
    //builder.addAllState(orSet.state.map(x => ByteString.copyFromUtf8(x.toString)).toIterable)
    // TODO serialize state

    builder
  }

  private def valueUpdatedFormat(valueUpdated: ValueUpdated): ValueUpdatedFormat.Builder =
    ValueUpdatedFormat.newBuilder.setOperation(payloadSerializer.payloadFormatBuilder(valueUpdated.operation.asInstanceOf[AnyRef]))

  private def updateOpFormatBuilder(op: UpdateOp): UpdateOpFormat.Builder =
    UpdateOpFormat.newBuilder.setDelta(payloadSerializer.payloadFormatBuilder(op.delta.asInstanceOf[AnyRef]))

  private def addOpFormatBuilder(op: AddOp): AddOpFormat.Builder =
    AddOpFormat.newBuilder.setEntry(payloadSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))

  private def removeOpFormatBuilder(op: RemoveOp): RemoveOpFormat.Builder = {
    RemoveOpFormat.newBuilder.setEntry(payloadSerializer.payloadFormatBuilder(op.entry.asInstanceOf[AnyRef]))
  }

  private def stableFormatBuilder(s: Stable): StableFormat.Builder = StableFormat.newBuilder.setTimestamp(payloadSerializer.payloadFormatBuilder(s.timestamp.asInstanceOf[VectorTime]))

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def polog(pologFormat: POLogFormat): POLog = {
    val rs = pologFormat.getVersionedEntriesList.iterator.asScala.foldLeft(Set.empty[Versioned[Operation]]) {
      case (acc, r) => acc + commonSerializer.versioned(r)
    }

    POLog(rs)
  }

  private def orSet(orSetFormat: ORSetFormat): ORSet[Any] = {
    val state: scala.collection.mutable.Buffer[Any] = orSetFormat.getStateList.asScala.map(x => {
      val bis = new ByteArrayInputStream(x.toByteArray)
      val in = new ObjectInputStream(bis)
      try {
        in.readObject()
      } finally {
        try {
          if (in != null) {
            in.close()
          }
        } catch {
          case e: IOException => println("IOException")
        }
      }
      null
    })
    ORSet(polog(orSetFormat.getPolog), state.toSet)
  }

  private def valueUpdated(valueUpdatedFormat: ValueUpdatedFormat): ValueUpdated =
    ValueUpdated(payloadSerializer.payload(valueUpdatedFormat.getOperation))

  private def updateOp(opFormat: UpdateOpFormat): UpdateOp =
    UpdateOp(payloadSerializer.payload(opFormat.getDelta))

  private def addOp(opFormat: AddOpFormat): AddOp =
    AddOp(payloadSerializer.payload(opFormat.getEntry))

  private def removeOp(opFormat: RemoveOpFormat): RemoveOp = {
    RemoveOp(payloadSerializer.payload(opFormat.getEntry))
  }

  private def stable(stableFormat: StableFormat): Stable =
    Stable(payloadSerializer.payload(stableFormat.getTimestamp))

}
