/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package integration.kafka.api

import com.yammer.metrics.core.Gauge
import kafka.api.IntegrationTestHarness
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals}
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters.MapHasAsScala

class OfflinePartitionsFromDeletedTopicTest extends IntegrationTestHarness {
  override protected def brokerCount: Int = 3

  @Test
  def testOfflinePartitionsBug(): Unit = {
    // create a topic with one partition per broker
    val topic = "test-topic"
    val numPartitions = brokerCount
    createTopicWithAssignment(topic, Map(
      0 -> List(0),
      1 -> List(1),
      2 -> List(2),
    ))

    // create some data
    val numRecords = 10
    val producer = createProducer()
    val futures = (0 until numRecords).map { i =>
      val partition = i % numPartitions
      producer.send(new ProducerRecord(topic, partition, i.toString.getBytes, i.toString.getBytes))
    }
    futures.map(_.get)
    producer.close()

    // there should be no offline partitions at this point
    assertEquals(0, getOfflinePartitionsCount, "")

    // shut down one of the brokers that is not the controller
    killBroker(if (zkClient.getControllerId.contains(2)) 1 else 2)

    // because the broker that's gone was the only ISR for one of the replicas there are now offline partitions
    assert(getOfflinePartitionsCount > 0)

    // there are only two brokers left, and one of them is the controller
    // the current controller's context counts offlinePartitions > 0,
    // let's trigger an election, something that can happen if the ZK session in the controller is lost
    electNewController

    // the new controller also counts offlinePartitions > 0,
    // but once we delete the topic that should go to 0
    deleteTopic(topic)
    assertEquals(0, getOfflinePartitionsCount, "Non zero offline partitions after deleting topic")

    // now let's elect another controller,
    // there's only another broker which is the previous controller
    // which is still caching offlinePartitions > 0 in its controllerContext,
    // it should change that count back to 0, but will it?
    electNewController
    assertEquals(0, getOfflinePartitionsCount, "Non zero offline partitions after old controller re-elected")
  }

  private def electNewController = {
    val oldController = zkClient.getControllerId
    var newController = oldController
    var tries = 10
    while (tries > 0 && oldController == newController) {
      zkClient.deletePath("/controller")
      newController = Some(TestUtils.waitUntilControllerElected(zkClient))
      tries -= 1
    }
    assertNotEquals(oldController, newController, "Failed to elect a different controller")
    print(s"Controller changed from ${oldController} to ${newController}")
  }

  private def getOfflinePartitionsCount: Int = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (k, _) => k.getName.endsWith("OfflinePartitionsCount") }.get._2
      .asInstanceOf[Gauge[Int]].value()
  }
}
