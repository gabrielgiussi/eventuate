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

package com.rbmhtechnology.eventuate.crdt

import akka.actor.Actor.emptyBehavior
import akka.actor.ActorRef
import com.rbmhtechnology.eventuate.EventsourcedView
import com.rbmhtechnology.eventuate.crdt.CRDTService.ValueUpdated

class CERDTAwaker(replicaId: String, crdtService: CRDTService[_, _], val eventLog: ActorRef) extends EventsourcedView {

  override def id: String = s"Awaker-${replicaId}"

  override def onCommand: Receive = emptyBehavior

  override def onEvent: Receive = {
    case ValueUpdated(op) if (!lastHandledEvent.processId.equals(lastHandledEvent.localLogId)) => {
      // Esta deberia ser un metodo en DurableEvent: lastCRDTId
      //val cerdtId = lastEmitterAggregateId.get.replaceFirst("CERMatch_","")
      val cerdtId = id.substring(id.indexOf("_") + 1, id.length)
      lastEmitterAggregateId.get.indexOf("_")
      println(s"Se recibio un ValueUpdated para el partido: ${cerdtId} " + op.toString)
      // Awake CRDTActor of the CERMatch in order to get him apologize
      crdtService.awake(cerdtId)
    }
  }
}
