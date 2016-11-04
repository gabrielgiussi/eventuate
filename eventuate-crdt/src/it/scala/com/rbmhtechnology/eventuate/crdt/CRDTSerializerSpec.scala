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

import akka.serialization.SerializationExtension
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTService.ValueUpdated
import com.rbmhtechnology.eventuate.serializer.DurableEventSerializerSpec._
import com.rbmhtechnology.eventuate.serializer.SerializationContext
import org.scalatest._

object CRDTSerializerSpec {
}

class CRDTSerializerSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  val context = new SerializationContext(
    MultiLocationConfig.create(),
    MultiLocationConfig.create(customConfig = serializerConfig),
    MultiLocationConfig.create(customConfig = serializerWithStringManifestConfig))

  override def afterAll(): Unit =
    context.shutdown()

  import context._

  "A CRDTSerializer" must {
    "support UpdateOp serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = UpdateOp(17)
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[UpdateOp]).get should be(expected)
    }
    "support ValueUpdated serialization with default payload serialization" in {
      val serialization = SerializationExtension(systems(0))

      val initial = ValueUpdated(UpdateOp(-10))
      val expected = initial

      serialization.deserialize(serialization.serialize(initial).get, classOf[ValueUpdated]).get should be(expected)
    }
  }
}
