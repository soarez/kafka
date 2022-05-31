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

package unit.kafka.server

import kafka.server.{BrokerToControllerChannelManager, ControllerRequestCompletionHandler, LogDirEventManager, LogDirEventManagerImpl}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common._
import org.apache.kafka.common.message.{AssignReplicasToDirectoriesRequestData, AssignReplicasToDirectoriesResponseData, LogDirectoriesOfflineRequestData, LogDirectoriesOfflineResponseData}
import org.apache.kafka.common.protocol.{ApiMessage, Errors}
import org.apache.kafka.common.requests._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, TimeUnit}
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

//noinspection ZeroIndexToHead
class LogDirEventManagerTest {

  val brokerId = 501
  @volatile var brokerEpoch: Int = 994
  val logDirs: Seq[Uuid] = Seq.fill(4)(Uuid.randomUuid)
  val topicPartitions: Seq[TopicPartition] =
    Seq.range(1, 3).map(n => new TopicPartition("foo", n)) ++
      Seq.range(1, 4).map(n => new TopicPartition("bar", n))
  var channelManager: BrokerToControllerChannelManager = _
  var logDirEventManager: LogDirEventManager = _
  var executor: ScheduledExecutorService = Executors.newScheduledThreadPool(5, (r: Runnable) => {
    val t = new Thread(r)
    t.setDaemon(true)
    t
  })

  @BeforeEach
  def setup(): Unit = {
    channelManager = mock(classOf[BrokerToControllerChannelManager])
    logDirEventManager = new LogDirEventManagerImpl(
      brokerId,
      () => brokerEpoch,
      channelManager
    )
  }

  @AfterEach
  def tearDown(): Unit = logDirEventManager.stop()

  // In this test we check that the manager will use batching of replicas assigned events
  // instead of sending a request to the controller for each queued event.
  // We also check that overridden events are discarded.
  @Test
  def testReplicasAssignedEventAreBatched(): Unit = {
    // Capture the first request to the controller
    @volatile var requestData: AssignReplicasToDirectoriesRequestData = null
    val readyToAssert = new CountDownLatch(1)
    when(channelManager.sendRequest(any(), any())).thenAnswer((invocation: InvocationOnMock) => {
      if (readyToAssert.getCount == 0) return // ignore subsequent requests
      readyToAssert.countDown()
      requestData = invocation.getArgument[AbstractRequest.Builder[AssignReplicasToDirectoriesRequest]](0).build().data()
    })

    logDirEventManager.replicaAssignedToLogDirectory(logDirs(1), topicPartitions(1))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(1), topicPartitions(2))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(3), topicPartitions(3))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(2), topicPartitions(2)) // should override earlier event for same partition
    logDirEventManager.start() // Start the manager late, to force batching of the queued events

    // Wait for the request to the controller
    assert(readyToAssert.await(250, TimeUnit.MILLISECONDS), "No request to the controller was made")
    assertAssignReplicasToDirectoriesRequestDataEquals(
      buildAssignReplicasToDirectoriesRequestData(Map(
        logDirs(1) -> Set(topicPartitions(1)),
        logDirs(2) -> Set(topicPartitions(2)),
        logDirs(3) -> Set(topicPartitions(3))
      )), requestData, "The request doesn't correctly represent the events")
  }

  // In this test we check that the manager will retry requests after a timeout.
  // We also check that the latest broker epoch is reflected in the request
  @Test
  def testRequeuesEventInTimedOutRequest(): Unit = {
    // Setup the brokerToControllerChannelManager to:
    //   - timeout the first request
    //   - bump the broker epoch after the first request
    //   - capture the data in the first two requests
    val requests = new mutable.ArrayBuffer[AssignReplicasToDirectoriesRequestData]()
    val readyToAssert = new CountDownLatch(2)
    when(channelManager.sendRequest(any(), any())).thenAnswer((invocation: InvocationOnMock) => {
      val requestNum = 3 - readyToAssert.getCount
      if (requestNum > 2) return // ignore requests after 2nd
      requests.addOne(invocation.getArgument[AbstractRequest.Builder[AssignReplicasToDirectoriesRequest]](0).build().data())
      if (requestNum == 1) {
        brokerEpoch += 1
        val completionHandler = invocation.getArgument[ControllerRequestCompletionHandler](1)
        executor.schedule(new Runnable {
          override def run(): Unit = completionHandler.onTimeout()
        }, 15, TimeUnit.MILLISECONDS)
      }
      readyToAssert.countDown()
    })

    logDirEventManager.replicaAssignedToLogDirectory(logDirs(1), topicPartitions(3))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(1), topicPartitions(4))
    logDirEventManager.start() // Start the manager late, to force batching of the queued events

    assert(readyToAssert.await(250, TimeUnit.MILLISECONDS), "No second request to the controller was made")
    assertAssignReplicasToDirectoriesRequestDataEquals(
      buildAssignReplicasToDirectoriesRequestData(
        Map(logDirs(1) -> Set(topicPartitions(3), topicPartitions(4)))
      ).setBrokerEpoch(brokerEpoch - 1),
      requests(0), "The first request doesn't correctly represent the events"
    )
    assertEquals(requests(0).brokerId(), requests(1).brokerId(), "The broker id changed")
    assertEquals(requests(0).brokerEpoch() + 1, requests(1).brokerEpoch(), "The broker epoch wasn't updated")
    assertEquals(requests(0).directories(), requests(1).directories(), "The second request doesn't propagate the same event")
  }

  // In this test we check that queued events are sent only once if the controller accepts them.
  @Test
  def testEventsAreSentOnce(): Unit = {
    // Setup the brokerToControllerChannelManager to:
    //   - acknowledge the first request
    //   - capture the data in the first two requests
    val requests = new mutable.ArrayBuffer[AssignReplicasToDirectoriesRequestData]()
    val readyToAssert = new CountDownLatch(2)
    val readyToSendSecondEvent = new CountDownLatch(1)
    when(channelManager.sendRequest(any(), any())).thenAnswer((invocation: InvocationOnMock) => {
      val requestNum = 3 - readyToAssert.getCount
      if (requestNum > 2) return // ignore requests after 2nd
      requests.addOne(invocation.getArgument[AbstractRequest.Builder[AssignReplicasToDirectoriesRequest]](0).build().data())
      if (requestNum == 1) { // only reply to the 1st request
        readyToSendSecondEvent.countDown()
        val completionHandler = invocation.getArgument[ControllerRequestCompletionHandler](1)
        val response = new AssignReplicasToDirectoriesResponse(buildAssignReplicasToDirectoriesResponseData(Map(
          logDirs(1) -> Set(topicPartitions(2))
        )))
        val clientResponse = mock(classOf[ClientResponse])
        when(clientResponse.responseBody()).thenReturn(response)
        executor.schedule(new Runnable {
          override def run(): Unit = completionHandler.onComplete(clientResponse)
        }, 15, TimeUnit.MILLISECONDS)
      }
      readyToAssert.countDown()
    })

    logDirEventManager.start() // Start the straight straight away
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(1), topicPartitions(2))
    assert(readyToSendSecondEvent.await(250, TimeUnit.MILLISECONDS), "Timed out waiting for first request")
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(2), topicPartitions(3))

    assert(readyToAssert.await(250, TimeUnit.MILLISECONDS), "No second request to the controller was made")
    assertAssignReplicasToDirectoriesRequestDataEquals(
      buildAssignReplicasToDirectoriesRequestData(Map(
        logDirs(1) -> Set(topicPartitions(2))
      )), requests(0), "The first request doesn't correctly represent the events")
    assertAssignReplicasToDirectoriesRequestDataEquals(buildAssignReplicasToDirectoriesRequestData(Map(
      logDirs(2) -> Set(topicPartitions(3))
    )), requests(1), "The second request doesn't correctly represent the events")
  }

  // In this test we check that the manager will use batching of log directories offline events
  // instead of sending a request to the controller for each queued event.
  @Test
  def testLogDirectoryOfflineEventsAreBatched(): Unit = {
    // Capture the first request to the controller
    @volatile var requestData: LogDirectoriesOfflineRequestData = null
    val readyToAssert = new CountDownLatch(1)
    when(channelManager.sendRequest(any(), any())).thenAnswer((invocation: InvocationOnMock) => {
      if (readyToAssert.getCount == 0) return // ignore subsequent requests
      readyToAssert.countDown()
      requestData = invocation.getArgument[AbstractRequest.Builder[LogDirectoriesOfflineRequest]](0).build().data()
    })

    logDirEventManager.replicaAssignedToLogDirectory(logDirs(0), topicPartitions(1))
    logDirEventManager.logDirectoryOffline(logDirs(1))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(0), topicPartitions(2))
    logDirEventManager.logDirectoryOffline(logDirs(2))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(0), topicPartitions(3))
    logDirEventManager.logDirectoryOffline(logDirs(3))

    logDirEventManager.start() // Start the manager late, to force batching of the queued events

    // Wait for the request to the controller
    assert(readyToAssert.await(250, TimeUnit.MILLISECONDS), "No request to the controller was made")
    assertLogDirectoriesOfflineRequestDataEquals(
      buildLogDirectoriesOfflineRequestData(Set(logDirs(1), logDirs(2), logDirs(3))),
      requestData, "The request doesn't correctly represent the events"
    )
  }

  // In this test we check log directory offline events are propagated first even if replica assignment events
  // are already queued.
  @Test
  def testLogDirectoryOfflineEventsTakePrecedence(): Unit = {
    // Capture the two first requests to the controller, reply to the first one with a LogDirectoriesOfflineResponse
    val requests = new mutable.ArrayBuffer[ApiMessage]()
    val readyToAssert = new CountDownLatch(2)
    when(channelManager.sendRequest(any(), any())).thenAnswer((invocation: InvocationOnMock) => {
      val requestNum = 3 - readyToAssert.getCount
      if (requestNum > 2) return // ignore requests after 2nd
      requests.addOne(invocation.getArgument[AbstractRequest.Builder[_ <: AbstractRequest]](0).build().data())
      if (requestNum == 1) { // only reply to the 1st request
        val completionHandler = invocation.getArgument[ControllerRequestCompletionHandler](1)
        val response = new LogDirectoriesOfflineResponse(
          buildLogDirectoriesOfflineResponseData(Set(logDirs(1), logDirs(2)))
        )
        val clientResponse = mock(classOf[ClientResponse])
        when(clientResponse.responseBody()).thenReturn(response)
        executor.schedule(new Runnable {
          override def run(): Unit = completionHandler.onComplete(clientResponse)
        }, 15, TimeUnit.MILLISECONDS)
      }
      readyToAssert.countDown()
    })

    logDirEventManager.replicaAssignedToLogDirectory(logDirs(0), topicPartitions(1))
    logDirEventManager.logDirectoryOffline(logDirs(1))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(0), topicPartitions(2))
    logDirEventManager.logDirectoryOffline(logDirs(2))
    logDirEventManager.replicaAssignedToLogDirectory(logDirs(3), topicPartitions(3))

    logDirEventManager.start() // Start the manager late, to force batching of the queued events

    // Wait for the request to the controller
    assert(readyToAssert.await(250, TimeUnit.MILLISECONDS), "No second request to the controller was made")
    assertLogDirectoriesOfflineRequestDataEquals(
      buildLogDirectoriesOfflineRequestData(Set(logDirs(1), logDirs(2))),
      requests(0).asInstanceOf[LogDirectoriesOfflineRequestData],
      "The first request doesn't correctly represent the events"
    )
    assertAssignReplicasToDirectoriesRequestDataEquals(
      buildAssignReplicasToDirectoriesRequestData(Map(
        logDirs(0) -> Set(topicPartitions(1), topicPartitions(2)),
        logDirs(3) -> Set(topicPartitions(3)),
      )),
      requests(1).asInstanceOf[AssignReplicasToDirectoriesRequestData],
      "The second request doesn't correctly represent the events"
    )
  }

  /*
   * TODO:
   *  - Test behavior upon per-partition errors for replicas to log dir assignment
   *  - Test behavior upon per-partition errors for log dirs offline
   */

  /**
   * Assert equality between two AssignReplicasToDirectoriesRequestData disregarding the order of any lists.
   */
  private def assertAssignReplicasToDirectoriesRequestDataEquals(
    a: AssignReplicasToDirectoriesRequestData,
    b: AssignReplicasToDirectoriesRequestData,
    message: String = null
  ): Unit = {
    def canonicalize(request: AssignReplicasToDirectoriesRequestData): AssignReplicasToDirectoriesRequestData =
      new AssignReplicasToDirectoriesRequestData()
        .setBrokerId(request.brokerId())
        .setBrokerEpoch(request.brokerEpoch())
        .setDirectories(
          request.directories().asScala
            .map(dir => new AssignReplicasToDirectoriesRequestData.DirectoryData().setId(dir.id())
              .setTopics(
                dir.topics().asScala
                  .map(topic => new AssignReplicasToDirectoriesRequestData.TopicData().setName(topic.name())
                    .setPartitions(
                      topic.partitions().asScala.sortBy(_.partitionIndex()).asJava
                    )
                  ).sortBy(_.name()).asJava
              )
            ).sortBy(_.id()).asJava
        )
    assertEquals(canonicalize(a), canonicalize(b), message)
  }

  /**
   * Assert equality between two LogDirectoriesOfflineRequestData disregarding the order of the list.
   */
  private def assertLogDirectoriesOfflineRequestDataEquals(
     a: LogDirectoriesOfflineRequestData,
     b: LogDirectoriesOfflineRequestData,
     message: String = null
   ): Unit = {
    def canonicalize(request: LogDirectoriesOfflineRequestData): LogDirectoriesOfflineRequestData =
      new LogDirectoriesOfflineRequestData()
        .setBrokerId(request.brokerId())
        .setBrokerEpoch(request.brokerEpoch())
        .setDirectories(request.directories().asScala.sortBy(_.id()).asJava)
    assertEquals(canonicalize(a), canonicalize(b), message)
  }

  private def buildLogDirectoriesOfflineRequestData(
    directories: Set[Uuid],
    brokerId: Int = brokerId,
    brokerEpoch: Long = brokerEpoch,
  ): LogDirectoriesOfflineRequestData =
    new LogDirectoriesOfflineRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch)
      .setDirectories(directories.map(new LogDirectoriesOfflineRequestData.DirectoryData().setId(_)).toSeq.asJava)

  private def buildAssignReplicasToDirectoriesRequestData(
    assignments: Map[Uuid, Set[TopicPartition]],
    brokerId: Int = brokerId,
    brokerEpoch: Long = brokerEpoch,
  ): AssignReplicasToDirectoriesRequestData =
    new AssignReplicasToDirectoriesRequestData()
      .setBrokerId(brokerId)
      .setBrokerEpoch(brokerEpoch)
      .setDirectories(assignments.map { case (dirId, topicPartitions) =>
        new AssignReplicasToDirectoriesRequestData.DirectoryData().setId(dirId).setTopics(
          topicPartitions.groupBy(_.topic()).map {
            case (topicName, topicPartitions) => new AssignReplicasToDirectoriesRequestData.TopicData().setName(topicName)
              .setPartitions(
                topicPartitions.map(_.partition())
                  .map(new AssignReplicasToDirectoriesRequestData.PartitionData().setPartitionIndex(_))
                  .toSeq.asJava
              )
          }.toSeq.asJava
        )
      }.toSeq.asJava)

  private def buildAssignReplicasToDirectoriesResponseData(assignments: Map[Uuid, Set[TopicPartition]]): AssignReplicasToDirectoriesResponseData =
    new AssignReplicasToDirectoriesResponseData()
      .setErrorCode(Errors.NONE.code())
      .setDirectories(assignments.map { case (dirId, topicPartitions) =>
        new AssignReplicasToDirectoriesResponseData.DirectoryData().setId(dirId).setTopics(
          topicPartitions.groupBy(_.topic()).map {
            case (topicName, topicPartitions) => new AssignReplicasToDirectoriesResponseData.TopicData()
              .setName(topicName).setPartitions(
              topicPartitions
                .map(_.partition())
                .map(new AssignReplicasToDirectoriesResponseData.PartitionData().setPartitionIndex(_))
                .toSeq.asJava
            )
          }.toSeq.asJava
        )
      }.toSeq.asJava)

  private def buildLogDirectoriesOfflineResponseData(logDirectoryIds: Set[Uuid]): LogDirectoriesOfflineResponseData =
    new LogDirectoriesOfflineResponseData()
    .setErrorCode(Errors.NONE.code())
    .setDirectories(
      logDirectoryIds.map(new LogDirectoriesOfflineResponseData.DirectoryData().setId(_)).toSeq.asJava
    )
}
