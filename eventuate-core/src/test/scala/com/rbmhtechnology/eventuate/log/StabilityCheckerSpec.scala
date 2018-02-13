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

import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.log.StabilityProtocol.MostRecentlyViewedTimestamps
import com.rbmhtechnology.eventuate.log.StabilityProtocol.StableVT
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

class StabilityCheckerSpec extends TestKit(ActorSystem("StabilityCheckerSpec")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with ImplicitSender {

  import StabilityProtocolSpecSupport._

  "StabilityChecker" should {
    "emit TCStable(0,0) when A = (1,0), B = unknown" in new ClusterAB {
      val st = stabilityChecker()
      st ! MostRecentlyViewedTimestamps(A, vt(1, 0))
      st ! StableVT
      expectMsg(tcstable(0, 0))
    }
    "emit TCStable(0,0) when A = (1,0), B = (0,0)" in new ClusterAB {
      val st = stabilityChecker()
      st ! MostRecentlyViewedTimestamps(A, vt(1, 0))
      st ! MostRecentlyViewedTimestamps(B, vt(0, 0))
      st ! StableVT
      expectMsg(tcstable(0, 0))
    }
    "emit TCStable(0,1) when A = (1,1), B = (0,1)" in new ClusterAB {
      val st = stabilityChecker()
      st ! MostRecentlyViewedTimestamps(A, vt(1, 1))
      st ! MostRecentlyViewedTimestamps(B, vt(0, 1))
      st ! StableVT
      expectMsg(tcstable(0, 1))
    }
    "emit TCStable(1,1) when A = (1,1), B = (1,1)" in new ClusterAB {
      val st = stabilityChecker()
      st ! MostRecentlyViewedTimestamps(A, vt(1, 1))
      st ! MostRecentlyViewedTimestamps(B, vt(1, 1))
      st ! StableVT
      expectMsg(tcstable(1, 1))
    }
    "emit TCStable(0,1,0) when A = (2,1,0), B = unknown, C = (1,1,1)" in new ClusterABC {
      val st = stabilityChecker()
      st ! MostRecentlyViewedTimestamps(A, vt(2, 1, 0))
      st ! MostRecentlyViewedTimestamps(C, vt(1, 1, 1))
      st ! StableVT
      expectMsg(tcstable(0, 1, 0))
    }
  }

}
