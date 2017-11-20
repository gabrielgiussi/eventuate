package com.rbmhtechnology.eventuate

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import com.rbmhtechnology.eventuate.ReplicationProtocol.ReplicationRead

class VectorTimeTracker(partitions: Set[String], stabilityChecker: ActorRef, props: Props) extends Actor {

  var trackedPartitions = Set.empty[String]
  val log = context.actorOf(props)

  def actorRefToString(ref: ActorRef): Option[String] = for {
    port <- ref.path.address.port
    host <- ref.path.address.host
  } yield s"neighbor[$host:$port]"

  override def receive = {
    case rr:ReplicationRead =>
      // support sparse networks. replicationreadenvelopes should say if it has seen all of its neighoburs.
      log forward rr
    case other => log forward other
  }
}
