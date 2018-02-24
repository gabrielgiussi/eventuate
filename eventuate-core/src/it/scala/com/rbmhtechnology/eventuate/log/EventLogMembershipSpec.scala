package com.rbmhtechnology.eventuate.log

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.testkit.TestKitBase
import akka.testkit.TestProbe
import com.rbmhtechnology.eventuate.EventsourcedActor
import com.rbmhtechnology.eventuate.EventsourcedView
import com.rbmhtechnology.eventuate.SingleLocationSpec
import com.rbmhtechnology.eventuate.log.EventLogMembershipProtocol.ConnectedEndpoint
import com.rbmhtechnology.eventuate.log.EventLogMembershipProtocol.EventLogMembership
import com.rbmhtechnology.eventuate.log.EventLogMembershipProtocol.UnconnectedPartition
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

import scala.util.Failure

object EventLogMembershipSpec2 {

  /**
   *      A - C
   *     /     \
   *    B       D
   */
  val partitionA = UnconnectedPartition("A", Set("B", "C"))
  val partitionB = UnconnectedPartition("B", Set("A"))
  val partitionC = UnconnectedPartition("C", Set("A", "D"))
  val partitionD = UnconnectedPartition("D", Set("C"))

  case class PersistUnconnectedPartition(u: UnconnectedPartition)

  class FakeEventLogMembershipActor(val id: String, val eventLog: ActorRef) extends EventsourcedActor {

    override def aggregateId: Option[String] = Some(EventLogMembershipProtocol.membershipAggregateId)

    override def onCommand: Receive = {
      case u: UnconnectedPartition => persist(u) {
        case Failure(t) => assert(false, s"This shouldn't happen, fix the test. Error: ${t.getMessage}")
        case _          => ()
      }
    }

    override def onEvent: Receive = Actor.emptyBehavior
  }

  class EventLogMembershipTestActor(logId: String, eventLog: ActorRef, connectedEndpoints: Int) extends EventLogMembershipActor(logId, eventLog, connectedEndpoints) {

  }

  class MembershipAwareActor(val id: String, val eventLog: ActorRef, probe: ActorRef) extends EventsourcedView {

    override def onCommand: Receive = Actor.emptyBehavior

    override def onEvent: Receive = {
      case EventLogMembership(members) => probe ! members
    }

  }

}

// TODO rename
trait EventLogMembershipSpec2 extends TestKitBase with WordSpecLike with Matchers with SingleLocationSpec {

  import EventLogMembershipSpec2._
  import scala.concurrent.duration._

  var actorId = 0

  override def beforeEach(): Unit = {
    super.beforeEach()
    actorId += 1
  }

  class MembershipContext(connections: Int) {
    val probe = TestProbe()
    // TODO create one or search the one that creates the eventLog?
    val membership = system.actorOf(Props(new EventLogMembershipActor("A", log, connections)))
    val fakeParticipant = system.actorOf(Props(new FakeEventLogMembershipActor(s"membershipFake-$actorId", log)))
    system.actorOf(Props(new MembershipAwareActor(s"membershipAware-$actorId", log, probe.ref)))
  }

  "An EventLogMembershipActor" must {
    "not reach agreement if it not see initial connections" in new MembershipContext(2) {
      membership ! ConnectedEndpoint("A", Some("A"))
      probe.expectNoMsg(3.seconds)
    }
    "b" in new MembershipContext(1) {
      membership ! ConnectedEndpoint("B")
      probe.expectMsg(Set("A"))
    }
    "c" in new MembershipContext(2) {
      membership ! ConnectedEndpoint("B", Some("B"))
      membership ! ConnectedEndpoint("C", Some("C"))
      fakeParticipant ! partitionB
      fakeParticipant ! partitionC
      fakeParticipant ! partitionD
      probe.expectMsg(Set("A", "B", "C", "D"))
    }
    "d" in new MembershipContext(2) {
      fakeParticipant ! partitionB
      fakeParticipant ! partitionC
      fakeParticipant ! partitionD
      Thread.sleep(2000) // TODO how to wait until the actors receives this replicated events?
      membership ! ConnectedEndpoint("B", Some("B"))
      membership ! ConnectedEndpoint("C", Some("C"))
      probe.expectMsg(Set("A", "B", "C", "D"))
    }
  }

  // TODO test recovery
}
