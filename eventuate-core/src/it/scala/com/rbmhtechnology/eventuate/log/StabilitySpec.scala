package com.rbmhtechnology.eventuate.log

import akka.actor.ActorSystem
import akka.testkit.TestKitBase

class StabilitySpec extends TestKitBase {

  override implicit val system: ActorSystem = ActorSystem("stabilitySpec")
}
