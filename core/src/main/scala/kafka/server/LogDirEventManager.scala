/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package kafka.server

import kafka.utils.{Logging, ShutdownableThread}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.{AssignReplicasToDirectoriesRequestData, LogDirectoriesOfflineRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AssignReplicasToDirectoriesRequest, AssignReplicasToDirectoriesResponse, LogDirectoriesOfflineRequest, LogDirectoriesOfflineResponse}
import org.apache.kafka.common.{TopicPartition, Uuid}

import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, Future, LinkedBlockingQueue}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}

/*
 * TODO
 *  - Add logging
 *  - Process per log directory
 */

trait LogDirEventManager {
  def replicaAssignedToLogDirectory(logDirId: Uuid, topicPartition: TopicPartition): Unit

  def logDirectoryOffline(logDirId: Uuid): Unit

  def start(): Unit

  def stop(): Unit
}

class LogDirEventManagerImpl(
                              val brokerId: Int,
                              val brokerEpochSupplier: () => Long,
                              brokerToControllerChannelManager: BrokerToControllerChannelManager
                            ) extends LogDirEventManager with Logging {

  sealed trait LogDirEvent

  case class LogDirectoriesOfflineEvent(logDirIds: Seq[Uuid]) extends LogDirEvent {
    def merge(other: LogDirectoriesOfflineEvent): LogDirectoriesOfflineEvent = LogDirectoriesOfflineEvent(logDirIds ++ other.logDirIds)
  }

  case class ReplicasAssignedEvent(directories: Map[Uuid, Map[String, Seq[Int]]], sequenceNumber: Long) extends LogDirEvent {
    def merge(other: ReplicasAssignedEvent): ReplicasAssignedEvent = {
      new ReplicasAssignedEvent(
        (individualAssignments() ++ other.individualAssignments()) // join individual assignments from both events
          .groupBy(_._2) // group by topic partition, so we can de-dupe assignments for the same partition
          .values.map(dupes => dupes.maxBy(_._3)) // select the dupe with the highest event index
          .groupBy(_._1) // group by log directory ID
          .view.mapValues(
          _.map(_._2) // take the topic partitions
            .groupBy(_.topic())
            .view.mapValues(_.map(_.partition()).toSeq).toMap // join partition indices in a Seq
        ).toMap,
        Math.max(sequenceNumber, other.sequenceNumber),
      )
    }

    private def individualAssignments(): Seq[(Uuid, TopicPartition, Long)] = {
      directories.flatMap {
        case (logDirId, topics) => topics.flatMap {
          case (topicName, partitions) => partitions.map(partition => (logDirId, new TopicPartition(topicName, partition), sequenceNumber))
        }
      }.toSeq
    }
  }

  object ReplicasAssignedEvent {
    def apply(topicPartition: TopicPartition, logDirId: Uuid, sequenceNumber: Long): ReplicasAssignedEvent = {
      ReplicasAssignedEvent(Map(logDirId -> Map(topicPartition.topic() -> Seq(topicPartition.partition()))), sequenceNumber)
    }
  }

  private val queue = new LinkedBlockingQueue[LogDirEvent]()
  private val replicaAssignmentSequenceNumber = new AtomicInteger(0)
  private val eventHandler: ShutdownableThread = new ShutdownableThread("LogDirEventHandler") {
    override def doWork(): Unit = processPendingEvents()
  }

  override def replicaAssignedToLogDirectory(logDirId: Uuid, topicPartition: TopicPartition): Unit = {
    queue.put(ReplicasAssignedEvent(topicPartition, logDirId, replicaAssignmentSequenceNumber.incrementAndGet()))
  }

  override def logDirectoryOffline(logDirId: Uuid): Unit = {
    queue.put(LogDirectoriesOfflineEvent(Seq(logDirId)))
  }

  override def start(): Unit = {
    eventHandler.start()
  }

  override def stop(): Unit = {
    eventHandler.shutdown()
  }

  private def processPendingEvents(): Unit = {
    val events = new util.ArrayList[LogDirEvent]()
    events.add(queue.take()) // block until there's any new event
    queue.drainTo(events) // but take all of the existing events

    // aggregate events by type
    val failures = events.asScala.filter(_.isInstanceOf[LogDirectoriesOfflineEvent]).map(_.asInstanceOf[LogDirectoriesOfflineEvent]).reduceOption(_.merge(_))
    val assignments = events.asScala.filter(_.isInstanceOf[ReplicasAssignedEvent]).map(_.asInstanceOf[ReplicasAssignedEvent]).reduceOption(_.merge(_))

    val requestCompleteFuture =
      if (failures.isDefined) { // if there are failures, deal with those first
        assignments.foreach(queue.put) // re-queue the aggregated assignment event
        Some(pushLogDirectoriesOfflineEventToController(failures.get))
      } else if (assignments.isDefined) {
        Some(pushReplicasAssignedEventToController(assignments.get))
      } else {
        None
      }
    requestCompleteFuture.foreach(_.get()) // block until the pending request finishes
  }

  private def pushLogDirectoriesOfflineEventToController(event: LogDirectoriesOfflineEvent): Future[Unit] = {
    val requestCompleteFuture = new CompletableFuture[Unit]()
    brokerToControllerChannelManager.sendRequest(
      new LogDirectoriesOfflineRequest.Builder(buildRequestData(event)),
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          try {
            if (response.authenticationException() != null) {
              error(s"Unable to propagate log directories offline event because of an authentication exception.",
                response.authenticationException())
              queue.put(event)
            } else if (response.versionMismatch() != null) {
              error(s"Unable to propagate log directories offline event because of an API version problem.",
                response.versionMismatch())
              queue.put(event)
            } else {
              val message = response.responseBody().asInstanceOf[LogDirectoriesOfflineResponse]
              val errorCode = Errors.forCode(message.data().errorCode())
              if (errorCode == Errors.NONE) {

                // for (dir: LogDirectoriesOfflineResponseData.DirectoryData <- message.data().directories()) {
                //   val error = dir.errorCode()
                //   val _ = dir.id()
                //   // TODO - decide how to deal with errors here
                // }

                info(s"Successfully propagated log directories offline event")
              } else {
                info(s"Unable to propagate log directories offline event because the controller returned " +
                  s"error $errorCode")
                queue.put(event)
              }
            }
          } finally {
            requestCompleteFuture.complete(())
          }
        }

        override def onTimeout(): Unit = {
          queue.put(event)
          requestCompleteFuture.complete(())
        }
      }
    )
    requestCompleteFuture
  }

  private def pushReplicasAssignedEventToController(event: ReplicasAssignedEvent): Future[Unit] = {
    val requestCompleteFuture = new CompletableFuture[Unit]()
    val requestData = buildRequestData(event)
    brokerToControllerChannelManager.sendRequest(
      new AssignReplicasToDirectoriesRequest.Builder(requestData),
      new ControllerRequestCompletionHandler {
        override def onComplete(response: ClientResponse): Unit = {
          var toRequeue: Option[LogDirEvent] = Some(event)
          try {
            if (response.authenticationException() != null) {
              error(s"Unable to propagate log directory assignment event because of an authentication exception.",
                response.authenticationException())
            } else if (response.versionMismatch() != null) {
              error(s"Unable to propagate log directory assignment event because of an API version problem.",
                response.versionMismatch())
            } else {
              val message = response.responseBody().asInstanceOf[AssignReplicasToDirectoriesResponse]
              val responseData = message.data()

              Errors.forCode(responseData.errorCode()) match {
                case Errors.STALE_BROKER_EPOCH =>
                  warn(s"Broker had a stale broker epoch (${requestData.brokerEpoch()}), retrying.")
                case Errors.CLUSTER_AUTHORIZATION_FAILED =>
                  error(s"Broker is not authorized to send AssignReplicasToDirectories to controller",
                    Errors.CLUSTER_AUTHORIZATION_FAILED.exception("Broker is not authorized to send AssignReplicasToDirectories to controller"))
                case Errors.NONE =>
                  val responseDataMap: Map[Uuid, Map[String, Map[Int, Errors]]] = responseData.directories().asScala.map(directoryData => (
                    directoryData.id(),
                    directoryData.topics().asScala.map(topicData => (
                      topicData.name(),
                      topicData.partitions().asScala.map(partitionData => (
                        partitionData.partitionIndex(),
                        Errors.forCode(partitionData.errorCode())
                      )).toMap
                    )).toMap
                  )).toMap

                  val toRetry = mutable.ArrayBuffer[ReplicasAssignedEvent]()
                  for (dir: AssignReplicasToDirectoriesRequestData.DirectoryData <- requestData.directories().asScala) {
                    for (topic: AssignReplicasToDirectoriesRequestData.TopicData <- dir.topics().asScala) {
                      for (partition: AssignReplicasToDirectoriesRequestData.PartitionData <- topic.partitions().asScala) {
                        val logDirId = dir.id()
                        val topicName = topic.name()
                        val partitionIdx = partition.partitionIndex()
                        val topicPartition = new TopicPartition(topic.name(), partitionIdx)
                        val result = responseDataMap.get(logDirId)
                          .flatMap(_.get(topicName))
                          .flatMap(_.get(partitionIdx))
                        var eventToRequeue: Option[ReplicasAssignedEvent] = Some(ReplicasAssignedEvent(topicPartition, logDirId, event.sequenceNumber))
                        result match {
                          case None => error(s"Controller ignored assignment of $topicPartition to directory $logDirId")
                          case Some(Errors.NONE) => eventToRequeue = None
                          case Some(e) => error(s"Failed to propagate assignment of $topicPartition to directory $logDirId due to $e")
                        }
                        eventToRequeue.foreach(toRetry.addOne)
                      }
                    }
                  }
                  if (toRetry.isEmpty) {
                    info(s"Successfully propagated log directory assignment event")
                  } else {
                    info(s"Partial success propagating log directory assignment event. Retrying ${toRetry.size} assignment events")
                  }
                  toRequeue = toRetry.reduceOption(_.merge(_))
                case otherError =>
                  info(s"Unable to propagate log directory assignment event because the controller returned " +
                    s"error $otherError")
              }
            }
          } finally {
            toRequeue.foreach(queue.put)
            requestCompleteFuture.complete(())
          }
        }

        override def onTimeout(): Unit = {
          queue.put(event)
          requestCompleteFuture.complete(())
        }
      })
    requestCompleteFuture
  }

  private def buildRequestData(event: LogDirectoriesOfflineEvent): LogDirectoriesOfflineRequestData =
    new LogDirectoriesOfflineRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setDirectories(event.logDirIds.map(
        new LogDirectoriesOfflineRequestData.DirectoryData().setId(_)
      ).asJava)

  private def buildRequestData(event: ReplicasAssignedEvent): AssignReplicasToDirectoriesRequestData =
    new AssignReplicasToDirectoriesRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpochSupplier.apply())
      .setDirectories(
        event.directories.map { case (logDirId, topics) =>
          new AssignReplicasToDirectoriesRequestData.DirectoryData().setId(logDirId)
            .setTopics(
              topics.map { case (topicName: String, partitions: Seq[Int]) =>
                new AssignReplicasToDirectoriesRequestData.TopicData()
                  .setName(topicName)
                  .setPartitions(
                    partitions
                      .map(index => new AssignReplicasToDirectoriesRequestData.PartitionData().setPartitionIndex(index))
                      .toList
                      .asJava
                  )
              }.toList.asJava
            )
        }.toList.asJava
      )
}
