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
import com.rbmhtechnology.eventuate.crdt.CRDTFormats._
import com.rbmhtechnology.eventuate.crdt.CRDTService._
import com.rbmhtechnology.eventuate.serializer.CommonSerializer

class CRDTSerializer(system: ExtendedActorSystem) extends Serializer {
  val commonSerializer = new CommonSerializer(system)
  import commonSerializer.payloadSerializer

  private val UpdatedOpClass = classOf[UpdateOp]
  private val ValueUpdatedClass = classOf[ValueUpdated]

  override def identifier: Int = 22567
  override def includeManifest: Boolean = true

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case v: ValueUpdated =>
      valueUpdatedFormat(v).build().toByteArray
    case o: UpdateOp =>
      updateOpFormatBuilder(o).build().toByteArray
    case _ =>
      throw new IllegalArgumentException(s"can't serialize object of type ${o.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = manifest match {
    case None => throw new IllegalArgumentException("manifest required")
    case Some(clazz) => clazz match {
      case ValueUpdatedClass =>
        valueUpdated(ValueUpdatedFormat.parseFrom(bytes))
      case UpdatedOpClass =>
        updateOp(UpdateOpFormat.parseFrom(bytes))
      case _ =>
        throw new IllegalArgumentException(s"can't deserialize object of type ${clazz}")
    }
  }

  // --------------------------------------------------------------------------------
  //  toBinary helpers
  // --------------------------------------------------------------------------------

  private def valueUpdatedFormat(valueUpdated: ValueUpdated): ValueUpdatedFormat.Builder =
    ValueUpdatedFormat.newBuilder.setOperation(payloadSerializer.payloadFormatBuilder(valueUpdated.operation.asInstanceOf[AnyRef]))

  private def updateOpFormatBuilder(op: UpdateOp): UpdateOpFormat.Builder =
    UpdateOpFormat.newBuilder.setDelta(payloadSerializer.payloadFormatBuilder(op.delta.asInstanceOf[AnyRef]))

  // --------------------------------------------------------------------------------
  //  fromBinary helpers
  // --------------------------------------------------------------------------------

  private def valueUpdated(valueUpdatedFormat: ValueUpdatedFormat): ValueUpdated =
    ValueUpdated(payloadSerializer.payload(valueUpdatedFormat.getOperation))

  private def updateOp(opFormat: UpdateOpFormat): UpdateOp =
    UpdateOp(payloadSerializer.payload(opFormat.getDelta))

}
