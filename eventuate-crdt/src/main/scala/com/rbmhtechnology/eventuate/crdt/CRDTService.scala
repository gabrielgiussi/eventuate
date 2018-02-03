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

import java.util.concurrent.TimeUnit

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.rbmhtechnology.eventuate._
import com.rbmhtechnology.eventuate.crdt.CRDTTypes._
import com.rbmhtechnology.eventuate.log.StabilityChannel.SubscribeTCStable
import com.rbmhtechnology.eventuate.log.StabilityProtocol.TCStable
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util._

/**
 * Typeclass to be implemented by CRDTs if they shall be managed by [[CRDTService]]
 *
 * @tparam A CRDT type
 * @tparam B CRDT value type
 */
trait CRDTServiceOps[A, B] {

  /**
   * Default CRDT instance.
   */
  def zero: A

  /**
   * Returns the CRDT value (for example, the entries of an OR-Set)
   */
  //def value(crdt: A): B = eval(crdt.polog, crdt.state)
  final def value(crdt: A): B = eval(crdt)

  def eval(crdt: A): B

  /**
   * Must return `true` if CRDT checks preconditions. Should be overridden to return
   * `false` if CRDT does not check preconditions, as this will significantly increase
   * write throughput.
   */
  def precondition: Boolean = true

  /**
   * Update phase 1 ("atSource"). Prepares an operation for phase 2.
   */
  def prepare(crdt: A, operation: Operation): Try[Option[Operation]] = Success(Some(operation))

  /**
   * Update phase 2 ("downstream").
   */
  def effect(crdt: A, op: Operation, vt: VectorTime, systemTimestamp: Long = 0L, creator: String = ""): A

  // TODO
  /**
   *
   *
   * This mechanism allows to discard stable operations, not only timestamps, if they have no
   * impact on the semantics of the data type. For some data types like RWSet
   * some operations become useless once other future operations become stable"
   * @param crdt a crdt
   * @param stable
   * @return the crdt after being applied causal stabilization. By default it returns the same crdt unmodified
   */
  def stable(crdt: A, stable: TCStable) = crdt

  def subscribeToStable: Boolean = false
}

object CRDTService {
  /**
   * Persistent event with update operation.
   *
   * @param operation update operation.
   */
  case class ValueUpdated(operation: Operation) extends CRDTFormat

}

private class CRDTServiceSettings(config: Config) {
  val operationTimeout: FiniteDuration =
    config.getDuration("eventuate.crdt.operation-timeout", TimeUnit.MILLISECONDS).millis
}

/**
 * A generic, replicated CRDT service that manages a map of CRDTs identified by name.
 * Replication is based on the replicated event `log` that preserves causal ordering
 * of events.
 *
 * @tparam A CRDT type
 * @tparam B CRDT value type
 */
trait CRDTService[A, B] {
  import CRDTService._

  private var manager: Option[ActorRef] = None

  private lazy val settings: CRDTServiceSettings =
    new CRDTServiceSettings(system.settings.config)

  private implicit lazy val timeout: Timeout =
    Timeout(settings.operationTimeout)

  /**
   * This service's actor system.
   */
  def system: ActorSystem

  /**
   * CRDT service id.
   */
  def serviceId: String

  /**
   * Event log.
   */
  def log: ActorRef

  /**
   * CRDT service operations.
   */
  def ops: CRDTServiceOps[A, B]

  /**
   * Starts the CRDT service.
   */
  def start(): Unit = if (manager.isEmpty) {
    manager = Some(system.actorOf(Props(new CRDTManager)))
  }

  /**
   * Stops the CRDT service.
   */
  def stop(): Unit = manager.foreach { m =>
    manager = None
    system.stop(m)
  }

  /**
   * Returns the current value of the CRDT identified by `id`.
   */
  def value(id: String): Future[B] = withManagerAndDispatcher { (w, d) =>
    w.ask(Get(id)).mapTo[GetReply].map(_.value)(d)
  }

  /**
   * Saves a snapshot of the CRDT identified by `id`.
   */
  def save(id: String): Future[SnapshotMetadata] = withManagerAndDispatcher { (w, d) =>
    w.ask(Save(id)).mapTo[SaveReply].map(_.metadata)(d)
  }

  /**
   * Updates the CRDT identified by `id` with given `operation`.
   * Returns the updated value of the CRDT.
   */
  def stable(id: String, t: VectorTime): Future[String] = withManagerAndDispatcher { (w, d) =>
    w.ask(MarkStable(id, t)).mapTo[MarkStableReply].map(_.id)(d)
  }

  def timestamps(id: String): Future[Set[VectorTime]] = withManagerAndDispatcher { (w, d) =>
    w.ask(GetTimestamps(id)).mapTo[GetTimestampsReply].map(_.timestamps)(d)
  }

  protected def op(id: String, operation: Operation): Future[B] = withManagerAndDispatcher { (w, d) =>
    w.ask(Update(id, operation)).mapTo[UpdateReply].map(_.value)(d)
  }

  private def withManagerAndDispatcher[R](async: (ActorRef, ExecutionContext) => Future[R]): Future[R] = manager match {
    case None    => Future.failed(new Exception("Service not started"))
    case Some(m) => async(m, system.dispatcher)
  }

  private trait Identified {
    def id: String
  }

  private case class Get(id: String) extends Identified
  private case class GetReply(id: String, value: B) extends Identified

  private case class Update(id: String, operation: Operation) extends Identified
  private case class UpdateReply(id: String, value: B) extends Identified

  private case class Save(id: String) extends Identified
  private case class SaveReply(id: String, metadata: SnapshotMetadata) extends Identified

  private case class MarkStable(id: String, timestamp: VectorTime) extends Identified

  private case class MarkStableReply(id: String) extends Identified

  private case class GetTimestamps(id: String) extends Identified

  case class GetTimestampsReply(timestamps: Set[VectorTime])

  private case class OnChange(crdt: A, operation: Option[Operation])

  private class CRDTActor(crdtId: String, override val eventLog: ActorRef) extends EventsourcedActor {
    override val id =
      s"${serviceId}_${crdtId}"

    override val aggregateId =
      Some(s"${ops.zero.getClass.getSimpleName}_${crdtId}")

    var crdt: A =
      ops.zero

    if (ops.subscribeToStable) eventLog ! SubscribeTCStable(self)

    override def stateSync: Boolean = ops.precondition

    override def onCommand = {
      case Get(`crdtId`) =>
        sender() ! GetReply(crdtId, ops.value(crdt))
      case Update(`crdtId`, operation) =>
        ops.prepare(crdt, operation) match {
          case Success(Some(op)) =>
            persist(ValueUpdated(op)) {
              case Success(evt) =>
                sender() ! UpdateReply(crdtId, ops.value(crdt))
              case Failure(err) =>
                sender() ! Status.Failure(err)
            }
          case Success(None) =>
            sender() ! UpdateReply(crdtId, ops.value(crdt))
          case Failure(err) =>
            sender() ! Status.Failure(err)
        }
      case Save(`crdtId`) =>
        save(crdt) {
          case Success(md) =>
            sender() ! SaveReply(crdtId, md)
          case Failure(err) =>
            sender() ! Status.Failure(err)
        }
      case s: TCStable =>
        crdt = ops.stable(crdt, s)
        onStable(crdt,s)
    }

    override def onEvent = {
      case evt @ ValueUpdated(operation) =>
        crdt = ops.effect(crdt, operation, lastHandledEvent.vectorTimestamp, lastHandledEvent.systemTimestamp, lastHandledEvent.emitterId)
        context.parent ! OnChange(crdt, Some(operation))
    }

    override def onSnapshot = {
      case snapshot =>
        crdt = snapshot.asInstanceOf[A]
        context.parent ! OnChange(crdt, None)
    }

  }

  private class CRDTManager extends Actor {
    var crdts: Map[String, ActorRef] = Map.empty

    def receive = {
      case cmd: Identified =>
        crdtActor(cmd.id) forward cmd
      case n @ OnChange(crdt, operation) =>
        onChange(crdt, operation)
      case Terminated(crdt) =>
        crdts.find(pair => pair._2 == crdt).map(_._1).foreach { id =>
          crdts = crdts - id
        }
    }

    def crdtActor(id: String): ActorRef = crdts.get(id) match {
      case Some(crdt) =>
        crdt
      case None =>
        val crdt = context.watch(context.actorOf(Props(new CRDTActor(id, log))))
        crdts = crdts.updated(id, crdt)
        crdt
    }
  }

  /** For testing purposes only */
  private[crdt] def onChange(crdt: A, operation: Option[Operation]): Unit = ()

  private[crdt] def onStable(crdt: A, stable: TCStable): Unit = ()
}
