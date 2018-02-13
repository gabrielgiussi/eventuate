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

case class EventuateNodeTest(name: String, connections: Set[String], logName: String, val role: RoleName) {

  private def logPartition(endpoint: String) = s"${endpoint}_${logName}"

  val partitionName = logPartition(name)

  def partitionName(connection: String) = logPartition(connection)

}

abstract class EventuateMultiNodeSpecConfig extends MultiNodeConfig {

  def logName: String

  def endpointNode(name: String, connections: Set[String]) = EventuateNodeTest(name, connections, logName, role(name))

}

abstract class EventuateMultiNodeSpec(config: EventuateMultiNodeSpecConfig) extends MultiNodeSpec(config) with MultiNodeWordSpec with MultiNodeReplicationEndpoint {

  override def logName: String = config.logName

  object Implicits {

    implicit def toRole(e: EventuateNodeTest) = e.role

    implicit class EnhancedEventuateNodeTest(e: EventuateNodeTest) {
      private def _run(thunk: ReplicationEndpoint => Unit): Unit = {
        val endpoint = createEndpoint(e.name, Set(e.logName), e.connections.map(c => node(RoleName(c)).address.toReplicationConnection))
        thunk(endpoint)
      }

      private def _runWith[T](thunk1: ReplicationEndpoint => T, thunk2: (ReplicationEndpoint, T) => Unit): Unit = {
        val endpoint = createEndpoint(e.name, Set(e.logName), e.connections.map(c => node(RoleName(c)).address.toReplicationConnection))
        val t = thunk1(endpoint)
        thunk2(endpoint, t)
      }

      def run(thunk: ReplicationEndpoint => Unit) = runOn(e.role)(_run(thunk))

      def runWith[T](thunk1: ReplicationEndpoint => T)(thunk2: (ReplicationEndpoint, T) => Unit) = runOn(e.role)(_runWith(thunk1, thunk2))

    }

  }

}
