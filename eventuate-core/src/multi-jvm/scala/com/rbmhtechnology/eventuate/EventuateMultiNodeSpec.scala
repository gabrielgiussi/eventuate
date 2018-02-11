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

import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec

case class EndpointTest(name: String, connections: Set[String], logName: String, val role: RoleName) {

  private def logPartition(endpoint: String) = s"${endpoint}_${logName}"

  val partitionName = logPartition(name)

  def partitionName(connection: String) = logPartition(connection)

}

abstract class EventuateMultiNodeSpecConfig extends MultiNodeConfig {

  def logName: String

  def endpointTest(name: String, connections: Set[String]) = EndpointTest(name, connections, logName, role(name))

}

abstract class EventuateMultiNodeSpec(config: EventuateMultiNodeSpecConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {

  override def logName: String = config.logName

  object Implicits {

    implicit class EndpointTest2(e: EndpointTest) {
      def _th(t: ReplicationEndpoint => Unit): Unit = {
        val endpoint = createEndpoint(e.name, Set(e.logName), e.connections.map(c => node(RoleName(c)).address.toReplicationConnection))
        t(endpoint)
      }

      def _th2[T](t: ReplicationEndpoint => T, t2: (ReplicationEndpoint, T) => Unit): Unit = {
        val endpoint = createEndpoint(e.name, Set(e.logName), e.connections.map(c => node(RoleName(c)).address.toReplicationConnection))
        val aux = t(endpoint)
        t2(endpoint, aux)
      }

      def run(thunk: ReplicationEndpoint => Unit) = runOn(e.role)(_th(thunk))

      def runWith[T](thunk1: ReplicationEndpoint => T)(thunk2: (ReplicationEndpoint, T) => Unit) = runOn(e.role)(_th2(thunk1, thunk2))

    }

  }

}
