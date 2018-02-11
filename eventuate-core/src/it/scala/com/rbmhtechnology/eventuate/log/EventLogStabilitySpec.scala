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

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.DurableEvent
import com.rbmhtechnology.eventuate.IntegrationTestException
import com.rbmhtechnology.eventuate.ReplicationFilter
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationMetadata
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationRead
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationWrite
import com.rbmhtechnology.eventuate.VectorTime
import com.rbmhtechnology.eventuate.log.EventLogStabilitySpec.LocalEventLog
import com.rbmhtechnology.eventuate.log.EventLogStabilitySpec.RemoteEventLog
import com.rbmhtechnology.eventuate.log.StabilityChannel.SubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityChannel.UnsubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityProtocol.MostRecentlyViewedTimestamps
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.WordSpecLike

import scala.collection.immutable
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class ForwardActor(to: ActorRef) extends Actor {
  override def receive: Receive = {
    case any => to forward any
  }
}

object EventLogStabilitySpec {
  val fromSequenceNrError = -1L
  val payloadError = "boom"

  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "DEBUG"
      |akka.log-dead-letters = off
      |eventuate.log.stability.partitions = [L,R1,R2]
    """.stripMargin)

  case class LocalEventLog(ref: ActorRef)

  case class LogState(eventLogClock: EventLogClock, deletionMetadata: DeletionMetadata) extends EventLogState

  class DummyEventLog(id: String) extends EventLog[LogState](id) {

    override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int, aggregateId: String): Future[BatchReadResult] = Future.successful(BatchReadResult(immutable.Seq.empty, 0L))

    override def replicationRead(fromSequenceNr: Long, toSequenceNr: Long, max: Int, scanLimit: Int, filter: DurableEvent => Boolean): Future[BatchReadResult] =
      if (fromSequenceNr equals fromSequenceNrError) Future.failed(IntegrationTestException) else Future.successful(BatchReadResult(immutable.Seq.empty, 0L))

    override def read(fromSequenceNr: Long, toSequenceNr: Long, max: Int): Future[BatchReadResult] = Future.successful(BatchReadResult(immutable.Seq.empty, 0L))

    override def write(events: immutable.Seq[DurableEvent], partition: Long, clock: EventLogClock): Unit =
      if (events.exists(_.payload equals payloadError)) throw IntegrationTestException else ()

    override def settings: EventLogSettings = new EventLogSettings {
      override def partitionSize: Long = 1

      override def initRetryMax: Int = 1

      override def initRetryDelay: FiniteDuration = FiniteDuration.apply(1, TimeUnit.SECONDS)

      override def deletionRetryDelay: FiniteDuration = FiniteDuration.apply(1, TimeUnit.SECONDS)
    }

    override def recoverState: Future[LogState] = Future.successful(LogState(EventLogClock(), DeletionMetadata(0L, Set.empty)))

    override def readReplicationProgresses: Future[Map[String, Long]] = ???

    override def readReplicationProgress(logId: String): Future[Long] = ???

    override def writeReplicationProgresses(progresses: Map[String, Long]): Future[Unit] = Future.successful(())

    override def writeDeletionMetadata(data: DeletionMetadata): Unit = ???

    override def writeEventLogClockSnapshot(clock: EventLogClock): Future[Unit] = ???

    //override def stabilityCheckerProps(partitions: Set[String]): Props = Props(classOf[ForwardActor], probeActor.ref)

  }

  def read(log: ActorRef, remoteId: String, vt: VectorTime, fail: Boolean = false) = {
    val fromSeqNr = if (fail) fromSequenceNrError else 0
    log.tell(ReplicationRead(fromSeqNr, 10, 10, ReplicationFilter.NoFilter, remoteId, Actor.noSender, vt), Actor.noSender)
  }

  def write(log: ActorRef, remoteId: String, events: Seq[VectorTime], fail: Boolean = false) = {
    val writes = events.map(vt => DurableEvent(payload = "", vectorTimestamp = vt, processId = remoteId))
    val metadata = events.fold(VectorTime.Zero)(_ merge _)
    val writes2 = if (fail) writes :+ DurableEvent(payload = payloadError) else writes
    log.tell(ReplicationWrite(writes2, Map(remoteId -> ReplicationMetadata(0, metadata)), false, Actor.noSender), Actor.noSender)
  }

  def write2(log: ActorRef, remoteId: String, remoteId2: String, events: Seq[VectorTime], fail: Boolean = false) = {
    val writes = events.map(vt => DurableEvent(payload = "", vectorTimestamp = vt, processId = remoteId2))
    val metadata = events.fold(VectorTime.Zero)(_ merge _)
    val writes2 = if (fail) writes :+ DurableEvent(payload = payloadError) else writes
    log.tell(ReplicationWrite(writes2, Map(remoteId -> ReplicationMetadata(0, metadata)), false, Actor.noSender), Actor.noSender)
  }

  case class RemoteEventLog(id: String) {
    private var cTVV = VectorTime.Zero

    def sendReplicationRead(cTVV: VectorTime, fail: Boolean = false)(implicit log: LocalEventLog) = {
      val fromSeqNr = if (fail) fromSequenceNrError else 0
      log.ref.tell(ReplicationRead(fromSeqNr, 10, 10, ReplicationFilter.NoFilter, id, null, cTVV), null)
    }

    def events(timestamps: Seq[VectorTime], fail: Boolean = false): Seq[DurableEvent] = {
      val writes = timestamps.map(vt => DurableEvent(payload = "", vectorTimestamp = vt, processId = id))
      val metadata = timestamps.fold(VectorTime.Zero)(_ merge _)
      //val writes2 = if (fail) writes :+ DurableEvent(payload = StabilitySpec.payloadError) else writes
      //log.tell(ReplicationWrite(writes2, Map(remoteId -> ReplicationMetadata(0, metadata)), false, system.deadLetters), system.deadLetters)
      writes // TODO

    }

    def expectedUpdate(cTVV: VectorTime) = MostRecentlyViewedTimestamps(id, cTVV)
  }
}

trait EventLogTest {
  val localId = "L"
  val remoteId1 = "R1"
  val remoteId2 = "R2"
  val TCStableZero = TCStable(VectorTime(localId -> 0, remoteId1 -> 0, remoteId2 -> 0))

  var _log: ActorRef = _
  val R1 = RemoteEventLog(remoteId1)
  val R2 = RemoteEventLog(remoteId2)

  def log: ActorRef = _log
  implicit def localEventLog = LocalEventLog(log)
  def vt(local: Long = 0, r1: Long = 0, r2: Long = 0) = VectorTime(localId -> local, remoteId1 -> r1, remoteId2 -> r2)
}

class EventLogStabilitySpec extends TestKit(ActorSystem("test", EventLogStabilitySpec.config)) with EventLogTest with WordSpecLike with BeforeAndAfterEach with BeforeAndAfterAll {

  // TODO test ReplicationWriteN
  import EventLogStabilitySpec._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    //_probeActor = TestProbe()
    _log = system.actorOf(Props(new DummyEventLog(localId) {
      override def sendRTMUpdates(updates: MostRecentlyViewedTimestamps): Unit = testActor ! updates
    }))
  }

  "An EventLog" should {
    "send RTM updates when receives ReplicationRead from a remote EventLog" in {
      R1.sendReplicationRead(cTVV = vt(local = 1, r1 = 0))
      expectMsg(R1.expectedUpdate(vt(local = 1, r1 = 0)))
      R1.sendReplicationRead(cTVV = vt(local = 1, r1 = 2))
      expectMsg(R1.expectedUpdate(vt(local = 1, r1 = 2)))
    }
    "send RTM updates when receives ReplicationRead from a remote EventLog even if it fails" in {
      R1.sendReplicationRead(cTVV = vt(local = 1, r1 = 3), fail = true)
      expectMsg(R1.expectedUpdate(vt(local = 1, r1 = 3)))
    }
  }
  "An EventLog" should {
    "send RTM updates when receives a ReplicationWrite" in {
      write(log, remoteId1, Seq(vt(local = 0, r1 = 1)))
      expectMsg(R1.expectedUpdate(vt(local = 0, r1 = 1)))
      expectMsg(MostRecentlyViewedTimestamps(Map(localId -> VectorTime(localId -> 0, remoteId1 -> 1, remoteId2 -> 0))))
    }
    "not send RTM update for its own entry if the write fails" in {
      write(log, remoteId1, Seq(vt(local = 0, r1 = 1)), fail = true)
      expectMsg(MostRecentlyViewedTimestamps(Map(remoteId1 -> vt(local = 0, r1 = 1))))
      expectNoMsg()
    }
    "send RTM updates for endpoints that are not the one that is sending the ReplicationWrite" in {
      write2(log, remoteId1, remoteId2, Seq(vt(local = 0, r1 = 1)))
      expectMsg(MostRecentlyViewedTimestamps(Map(remoteId2 -> vt(local = 0, r1 = 1))))
      expectMsg(MostRecentlyViewedTimestamps(Map(localId -> VectorTime(localId -> 0, remoteId1 -> 1, remoteId2 -> 0))))
    }
  }

}

class EventLogStabilitySpec2 extends TestKit(ActorSystem("test", EventLogStabilitySpec.config)) with EventLogTest with WordSpecLike with BeforeAndAfterEach with BeforeAndAfterAll {

  import EventLogStabilitySpec._

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    _log = system.actorOf(Props(new DummyEventLog(localId)))
  }

  "Subscribers" should {
    "receive TCStable" in {
      log ! SubscribeTCStable(testActor)
      R1.sendReplicationRead(cTVV = vt(local = 1, r1 = 0, r2 = 0))
      expectMsg(TCStableZero) // TODO maybe this should be filtered (in the channel or in the checker?)

      R2.sendReplicationRead(cTVV = vt(local = 2, r1 = 0, r2 = 0))
      expectMsg(TCStable(VectorTime(localId -> 1, remoteId1 -> 0, remoteId2 -> 0)))

      R1.sendReplicationRead(cTVV = vt(local = 2, r1 = 0, r2 = 0))
      expectMsg(TCStable(VectorTime(localId -> 2, remoteId1 -> 0, remoteId2 -> 0)))
    }
    "not receive duplicate TCStable" in {
      log ! SubscribeTCStable(testActor)
      R1.sendReplicationRead(cTVV = vt(local = 1, r1 = 0, r2 = 0))
      expectMsg(TCStableZero)

      R2.sendReplicationRead(cTVV = vt(local = 1, r1 = 0, r2 = 0))
      expectMsg(TCStable(VectorTime(localId -> 1, remoteId1 -> 0, remoteId2 -> 0)))

      R2.sendReplicationRead(cTVV = vt(local = 1, r1 = 1, r2 = 0))
      expectNoMsg()
    }
    "not receive more TCStable if they are unsubscribed" in {
      log ! SubscribeTCStable(testActor)
      R1.sendReplicationRead(cTVV = vt(local = 1, r1 = 0, r2 = 0))
      expectMsg(TCStableZero)
      R2.sendReplicationRead(cTVV = vt(local = 1, r1 = 0, r2 = 0))
      expectMsg(TCStable(VectorTime(localId -> 1, remoteId1 -> 0, remoteId2 -> 0)))

      log ! UnsubscribeTCStable(testActor)
      R1.sendReplicationRead(cTVV = vt(local = 2, r1 = 0, r2 = 0))
      R2.sendReplicationRead(cTVV = vt(local = 2, r1 = 0, r2 = 0))
      expectNoMsg()
    }
  }

}

