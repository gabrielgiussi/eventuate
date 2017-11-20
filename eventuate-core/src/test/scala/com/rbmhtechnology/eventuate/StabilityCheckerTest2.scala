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
import akka.actor.Props
import akka.testkit.TestKit
import akka.util.Timeout
import com.rbmhtechnology.eventuate.StabilityChecker2.MostRecentlyViewedTimestamp
import com.rbmhtechnology.eventuate.StabilityChecker2.StabilityCheck
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import scala.concurrent.Await
import scala.concurrent.Future
import scala.util.Failure

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
object StabilityCheckerTest2 {

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

class StabilityCheckerTest2 extends WordSpecLike with Matchers {

  import StabilityCheckerTest2._
  import StabilityChecker2._

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
}

class StabilityCheckerTest2Actor extends TestKit(ActorSystem("stabilityTest")) with WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  import StabilityCheckerTest2._
  import StabilityChecker2._
  import scala.concurrent.duration._
  import akka.pattern.ask

  override def afterAll(): Unit =
    TestKit.shutdownActorSystem(system)

  trait StabilityCheckContext {
    implicit val sender = testActor
    val stabilityChecker = system.actorOf(Props[StabilityChecker2])
  }

  "StabilityChecker" must {
    "not trigger an stability check if it wasn't enabled" in new StabilityCheckContext {
      stabilityChecker ! MostRecentlyViewedTimestamp("A", vt"A = 1, B = 1")
      stabilityChecker ! MostRecentlyViewedTimestamp("B", vt"A = 1, B = 1")
      stabilityChecker ! WritedEvents(Set(vt"A = 1, B = 0", vt"A = 0, B = 1"))
      stabilityChecker ! TriggerStabilityCheck
      expectNoMsg(1.second)
    }
    "not return stable events if it's enabled but there are none" in new StabilityCheckContext {
      stabilityChecker ! MostRecentlyViewedTimestamp("A", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! MostRecentlyViewedTimestamp("B", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! MostRecentlyViewedTimestamp("C", vt"A = 0, B = 0, C = 0")
      stabilityChecker ! WritedEvents(Set(vt"A = 1, B = 0, C = 0", vt"A = 0, B = 1, C = 0"))
      stabilityChecker ! Enable
      stabilityChecker ! TriggerStabilityCheck
      expectNoMsg(1.second)
    }
    "return stable events if it's enabled" in new StabilityCheckContext {
      stabilityChecker ! MostRecentlyViewedTimestamp("A", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! MostRecentlyViewedTimestamp("B", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! MostRecentlyViewedTimestamp("C", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! WritedEvents(Set(vt"A = 1, B = 0, C = 0", vt"A = 0, B = 1, C = 0"))
      stabilityChecker ! Enable
      stabilityChecker ! TriggerStabilityCheck
      expectMsg(1.second, TCStable(Set(vt"A = 1, B = 0, C = 0", vt"A = 0, B = 1, C = 0")))
    }
    "return only those events that are stable" in new StabilityCheckContext {
      implicit val timeout = Timeout(1 seconds)
      import system.dispatcher
      stabilityChecker ! MostRecentlyViewedTimestamp("A", vt"A = 2, B = 1, C = 0")
      stabilityChecker ! MostRecentlyViewedTimestamp("B", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! MostRecentlyViewedTimestamp("C", vt"A = 1, B = 1, C = 0")
      stabilityChecker ! WritedEvents(Set(vt"A = 1, B = 0, C = 0", vt"A = 0, B = 1, C = 0", vt"A = 2, B = 1, C = 0"))
      stabilityChecker ! Enable
      stabilityChecker ! TriggerStabilityCheck
      expectMsg(1.second, TCStable(Set(vt"A = 1, B = 0, C = 0", vt"A = 0, B = 1, C = 0")))
      val result: Future[Boolean] = stabilityChecker ? TriggerLightweightStabilityCheck map {
        case stableFilter: TCStableFilter =>
          val eventsToCheck = Set(vt"A = 1, B = 0, C = 0", vt"A = 0, B = 1, C = 0")
          eventsToCheck.forall(stableFilter)
        case _ => false
      }
      assert(Await.result(result,2 second))
    }
  }

}
