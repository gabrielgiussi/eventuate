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

package com.rbmhtechnology.eventuate.log

import com.rbmhtechnology.eventuate.VectorTime
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class StabilityProtocolSpec extends WordSpecLike with Matchers {

  import StabilityProtocol._
  import StabilityProtocolSpecSupport._

  implicit class EnhancedRTM(rtm: RTM) {
    private def _update(timestamps: Map[String, VectorTime]) = updateRTM(rtm)(timestamps)
    def update(e: (String, VectorTime), es: (String, VectorTime)*) = _update(Map(e) ++ Map(es: _*))
    def update(id: String, vt: VectorTime): RTM = _update(Map(id -> vt))
    def stable() = stableVectorTime(rtm.values.toSeq)
  }

  "Stability" should {
    "emit TCStable(0,0) when A = (1,0), B = unknown" in new ClusterAB {
      initialRTM
        .update(A, vt(1, 0))
        .stable shouldBe tcstable(0, 0)
    }
    "emit TCStable(0,1) when A = (1,1), B = (0,1)" in new ClusterAB {
      initialRTM
        .update(A, vt(1, 1))
        .update(B, vt(0, 1))
        .stable shouldBe tcstable(0, 1)
    }
    "emit TCStable(1,1) when A = (1,1), B = (1,1)" in new ClusterAB {
      initialRTM
        .update(A, vt(1, 1))
        .update(B, vt(1, 1))
        .stable shouldBe tcstable(1, 1)
    }
    "emit TCStable(0,1,0) when A = (1,1,1), B = unknown, C = (1,1,1)" in new ClusterABC {
      initialRTM
        .update(A, vt(1, 1, 1))
        .update(C, vt(1, 1, 1))
        .stable shouldBe tcstable(0, 1, 0)
    }
    "emit TCStable(1,1,1) when A = (2,1,1), B = (1,2,1), C = (1,1,2)" in new ClusterABC {
      initialRTM
        .update(
          (A, vt(2, 1, 1)),
          (B, vt(1, 2, 1)),
          (C, vt(1, 1, 2)))
        .stable shouldBe tcstable(1, 1, 1)
    }
  }

}
