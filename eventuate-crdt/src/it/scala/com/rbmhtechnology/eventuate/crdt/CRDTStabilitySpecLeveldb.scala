package com.rbmhtechnology.eventuate.crdt

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb
import com.rbmhtechnology.eventuate.crdt.AWSetService.AWSet
import com.rbmhtechnology.eventuate.log.EventLogSpecLeveldb
import com.rbmhtechnology.eventuate.log.EventLogTest
import com.rbmhtechnology.eventuate.log.StabilityProtocol
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers
import org.scalatest.WordSpecLike

object CRDTStabilitySpecLeveldb {

  // This config should be tied to the trait EventLogTest
  val config: Config = ConfigFactory.parseString(
    """
      |akka.loglevel = "DEBUG"
      |akka.log-dead-letters = off
      |eventuate.log.stability.partitions = [L,R1,R2]
    """.stripMargin).withFallback(EventLogSpecLeveldb.config)
}

class CRDTStabilitySpecLeveldb extends TestKit(ActorSystem("test", CRDTStabilitySpecLeveldb.config)) with EventLogTest with WordSpecLike with Matchers with SingleLocationSpecLeveldb {

  override def log: ActorRef = super[SingleLocationSpecLeveldb].log

  override def logId = localId

  case class StableCRDT[A](at: TCStable, value: A, pologSize: Int)

  "A CRDTService" must {
    "manage multiple CRDTs identified by name" in {
      val service = new AWSetService[Int]("a", log) {
        override private[crdt] def onStable(crdt: AWSet[Int], stable: StabilityProtocol.TCStable): Unit = {
          if (!stable.equals(TCStableZero)) testActor ! StableCRDT(stable, ops.value(crdt), crdt.polog.log.size)
        }
      }
      service.add("awset1", 1)
      service.add("awset1", 2)
      expectNoMsg()
      R1.sendReplicationRead(cTVV = vt(local = 1))
      expectNoMsg()
      R2.sendReplicationRead(cTVV = vt(local = 1))
      expectMsg(StableCRDT(TCStable(vt(1, 0, 0)), Set(1, 2), 1))

      R1.sendReplicationRead(cTVV = vt(local = 2))
      expectNoMsg()
      R2.sendReplicationRead(cTVV = vt(local = 2))
      expectMsg(StableCRDT(TCStable(vt(2, 0, 0)), Set(1, 2), 0))
    }
  }

}
