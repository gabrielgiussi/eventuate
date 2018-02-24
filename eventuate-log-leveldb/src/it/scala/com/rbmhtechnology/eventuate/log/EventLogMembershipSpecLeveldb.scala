package com.rbmhtechnology.eventuate.log

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.rbmhtechnology.eventuate.SingleLocationSpecLeveldb

class EventLogMembershipSpecLeveldb extends TestKit(ActorSystem("test")) with EventLogMembershipSpec2 with SingleLocationSpecLeveldb
