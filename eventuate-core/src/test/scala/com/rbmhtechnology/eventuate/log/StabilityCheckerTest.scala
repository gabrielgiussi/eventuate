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

package com.rbmhtechnology.eventuate

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.log.StabilityChecker
import com.rbmhtechnology.eventuate.log.StabilityChecker._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

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
object StabilityCheckerTest {

  val nodeNames = List("A", "B", "C", "D", "E", "F")

  implicit class EnhancedVectorTime(vt: VectorTime) {
    def stable(implicit matrix: Seq[VectorTime], stabilityCheck: StabilityCheck): Boolean = stabilityCheck(matrix)(vt)

    def noStable(implicit matrix: Seq[VectorTime], stabilityCheck: StabilityCheck): Boolean = !stable
  }

  implicit class StringToTimestamp(val sc: StringContext) {
    private def stringToPair(s: String): (String, Long) = s.split("=").map(_.trim).toList match {
      case name :: number :: Nil => name -> number.toLong
      case _                     => throw new RuntimeException("Wrong matrix format")
    }

    private def stringToVectorTimeWithNames(s: String): VectorTime = VectorTime(s.split(",").toSeq.map(stringToPair): _*)
    private def stringToVectorTime(s: String): VectorTime = VectorTime(nodeNames.zip(s.split(",").map(_.trim.toLong).toList): _*)

    private def string = sc.s().stripMargin.trim

    def rtm(): Seq[VectorTime] = {
      val lines = string.split(System.lineSeparator()).toSeq.map(_.trim)
      lines.map(stringToVectorTimeWithNames)
    }

    def svt(): VectorTime = stringToVectorTime(string)

    def vt(): VectorTime = stringToVectorTimeWithNames(string)
  }

}

class StabilityCheckerTest extends WordSpecLike with Matchers {

  import StabilityChecker._
  import StabilityCheckerTest._

  implicit val stc = stabilityCheck

  "StabilityCheck" should {
    // TODO review this test
    "return false if the VectorTime is not complete" in {
      implicit val rtm =
        rtm"""
             |A = 1, B = 1
             |A = 1, B = 1
             |A = 1, B = 1, C = 1
        """
      assert(vt"A = 1, B = 1, C = 1".noStable)
    }
  }

  "Georges Younes" in {
    implicit val rtm =
      rtm"""
           |A = 2, B = 1, C = 1
           |A = 0, B = 1, C = 0
           |A = 0, B = 0, C = 1
      """
    assert(svt"1, 0, 0".noStable)
    assert(svt"2, 0, 0".noStable)
    assert(svt"0, 0, 1".noStable)
    assert(svt"0, 1, 0".noStable)
  }
  "StableVector" in {
    implicit val rtm =
      rtm"""
           |A = 1, B = 1
           |A = 1, B = 1
      """
    stableVectorTime(rtm) shouldBe vt"A = 1, B = 1"
  }
}

class StabilityCheckerTestActor extends TestKit(ActorSystem("stabilityTest")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  import StabilityChecker._
  import StabilityCheckerTest._

  import scala.concurrent.duration._
  import scala.language.postfixOps

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  class StabilityCheckContext(partitions: Set[String]) {
    implicit val sender = testActor
    val stabilityChecker = system.actorOf(StabilityChecker.props(partitions))
  }

  "StabilityChecker" must {
    "not receive TCStable" in new StabilityCheckContext(Set("A", "B")) {
      stabilityChecker ! MostRecentlyViewedTimestamps("A", vt"A = 1, B = 0")
      stabilityChecker ! MostRecentlyViewedTimestamps("B", vt"A = 0, B = 1")
      stabilityChecker ! TriggerStabilityCheck
      expectNoMsg(100 millis)
      stabilityChecker ! MostRecentlyViewedTimestamps("A", vt"A = 3, B = 0")
      stabilityChecker ! MostRecentlyViewedTimestamps("B", vt"A = 0, B = 5")
      stabilityChecker ! TriggerStabilityCheck
      expectNoMsg(100 millis)
    }
    "receive TCStable" in new StabilityCheckContext(Set("A", "B")) {
      stabilityChecker ! MostRecentlyViewedTimestamps("A", vt"A = 1, B = 1")
      stabilityChecker ! MostRecentlyViewedTimestamps("B", vt"A = 1, B = 1")
      stabilityChecker ! TriggerStabilityCheck
      expectMsg(100 millis, TCStable(vt"A = 1, B = 1"))
      expectNoMsg(100 millis)
    }
    "receive TCStable2" in new StabilityCheckContext(Set("A", "B")) {
      stabilityChecker ! MostRecentlyViewedTimestamps("A", vt"A = 3, B = 1")
      stabilityChecker ! MostRecentlyViewedTimestamps("B", vt"A = 1, B = 2")
      stabilityChecker ! TriggerStabilityCheck
      expectMsg(100 millis, TCStable(vt"A = 1, B = 1"))
      expectNoMsg(100 millis)
      stabilityChecker ! MostRecentlyViewedTimestamps("A", vt"A = 3, B = 2")
      stabilityChecker ! TriggerStabilityCheck
      expectMsg(100 millis, TCStable(vt"A = 1, B = 2"))
      expectNoMsg(100 millis)
      stabilityChecker ! MostRecentlyViewedTimestamps("B", vt"A = 3, B = 2")
      stabilityChecker ! TriggerStabilityCheck
      expectMsg(100 millis, TCStable(vt"A = 3, B = 2"))
      expectNoMsg(100 millis)
    }
  }

}
