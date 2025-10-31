package com.example.cdc

import com.example.cdc.service.message.MessageRequestDto
import com.example.cdc.service.message.MessageService
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.event.RecordApplicationEvents
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Import(TestcontainersConfiguration::class)
@SpringBootTest
@RecordApplicationEvents
class CdcApplicationTests @Autowired constructor(
    private val kafka: KafkaContainer,
    private val messageService: MessageService,
    private val messageEventTestListener: MessageEventTestListener,
) {

	@Test
	fun `message events received`() {
        val createRequest = MessageRequestDto(message = "Hello World!", username = "Test User")
        val created = messageService.upsertMessage(createRequest)
        assertThat(created.id).isNotNull()
        assertThat(created.createTime).isEqualTo(created.updateTime)
        val id = created.id!!

        val firstMessage = messageEventTestListener.awaitMessageById(id)
        assertThat(firstMessage).isEqualTo(created)

        messageEventTestListener.removeMessageById(id)

        val updateRequest = createRequest.copy(id = id, message = "Hello Again!")
        val updated = messageService.upsertMessage(updateRequest)
        assertThat(updated.id).isEqualTo(created.id)
        assertThat(updated.createTime).isEqualTo(created.createTime)
        assertThat(updated.updateTime).isNotEqualTo(updated.createTime)

        val secondMessage = messageEventTestListener.awaitMessageById(id)
        assertThat(secondMessage).isEqualTo(updated)

        messageService.deleteMessage(id)
        assertThat(messageEventTestListener.awaitMessageDeleteById(id)).isTrue()

        parallelRandomAccessIsSerial()
	}

    private fun parallelRandomAccessIsSerial() {
        fun createKafkaConsumer(): KafkaConsumer<String, String> {
            val props = Properties().apply {
                this["bootstrap.servers"] = "${kafka.host}:${kafka.getMappedPort(9092)}"
                this["enable.auto.commit"] = "false"
                this["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
                this["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
                this["fetch.max.wait.ms"] = "100"
                this["max.poll.records"] = "1"
                this["default.api.timeout.ms"] = "3000"
            }
            return KafkaConsumer<String, String>(props)
        }

        val note = Vector<Pair<Instant, Long>>()
        fun KafkaConsumer<String, String>.read(tp: TopicPartition, offset: Long) {
            seek(tp, offset)
            val records = poll(Duration.ofMillis(100))
            val metrics = metrics()
            fun getMetric(name: String): Double =
                (metrics.entries.firstOrNull { it.key.name() == name }?.value?.metricValue() as? Double) ?: 0.0
            val connections = getMetric("connection-count")
            println("Connection count: $connections")
            if (connections > 0) {
                note.add(Pair(Instant.now(), offset))
                println("Partition ${tp.partition()} read: ${records.count()} records")
            } else {
                println("Connection error")
            }
        }

        val pool = KafkaConsumerPool(::createKafkaConsumer, poolSize = 5)
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val latch = CountDownLatch(20)
        repeat(20) {
            executor.submit {
                try {
                    pool.useConsumer(TopicPartition("messages.public.messages", 0)) { consumer, tp ->
                        consumer.read(tp, 0L)
                        consumer.read(tp, 1L)
                        consumer.read(tp, 2L)
                        consumer.read(tp, 0L)
                        // kafka.stop()
                    }
                }
                catch (t: Throwable) {
                    t.printStackTrace()
                }
                finally {
                    latch.countDown()
                }
            }
        }
        latch.await()
        pool.close()
        executor.shutdown()

        assertThat(note.size).isEqualTo(80)
        for (i in 0 until note.size - 1) {
            val current = note[i].first
            val next = note[i + 1].first
            assertThat(next).isAfter(current)
        }
    }

}

class KafkaConsumerPool<K, V>(
    private val factory: () -> KafkaConsumer<K, V>,
    private val poolSize: Int
) {
    private val consumers = mutableListOf<PooledConsumer<K, V>>()
    private val assignments = mutableMapOf<TopicPartition, PooledConsumer<K, V>>()
    private val lastUsed = LinkedHashMap<PooledConsumer<K, V>, Long>(poolSize, 0.75f, true)

    private val lock = ReentrantLock(false)
    private val semaphore = Semaphore(poolSize, true)

    private data class PooledConsumer<K, V>(
        val consumer: KafkaConsumer<K, V>,
        val lock: Lock,
    )

    fun <R> useConsumer(
        topicPartition: TopicPartition,
        block: (KafkaConsumer<K, V>, TopicPartition) -> R
    ): R {
        semaphore.acquire()
        try {
            val pooled = lock.withLock {
                val pooled = assignments[topicPartition]
                    ?: consumers.firstOrNull { it !in assignments.values }
                    ?: if (consumers.size < poolSize) {
                        createPooledConsumer()
                    } else {
                        lastUsed.keys.first()
                    }
                lastUsed[pooled] = System.nanoTime()
                pooled
            }

            val (result, connectionCount) = pooled.lock.withLock {
                if (assignments[topicPartition] != pooled) {
                    pooled.consumer.assign(listOf(topicPartition))
                    assignments.entries.removeIf { it.value == pooled }
                    assignments[topicPartition] = pooled
                }
                val result = block(pooled.consumer, topicPartition)
                val connectionCount = pooled.readConnectionCount()
                Pair(result, connectionCount)
            }

            if (connectionCount == 0.0) {
                lock.withLock {
                    releasePooledConsumer(pooled)
                }
            }

            return result
        } finally {
            semaphore.release()
        }
    }

    fun close() {
        lock.withLock {
            consumers.forEach { it.tryClose() }
            consumers.clear()
        }
    }

    private fun createPooledConsumer(): PooledConsumer<K, V> {
        val consumer = factory()
        val pooledConsumer = PooledConsumer(consumer, ReentrantLock(true))
        consumers += pooledConsumer
        lastUsed[pooledConsumer] = System.nanoTime()
        return pooledConsumer
    }

    private fun releasePooledConsumer(pooled: PooledConsumer<K, V>) {
        pooled.tryClose()
        consumers.remove(pooled)
        assignments.entries.removeIf { it.value == pooled }
        lastUsed.remove(pooled)
    }

    private fun PooledConsumer<K, V>.tryClose() {
        try {
            consumer.close()
        } catch (_: Exception) {
        }
    }

    private fun PooledConsumer<K, V>.readConnectionCount(): Double {
        val metrics = consumer.metrics()
        fun getMetric(name: String): Double =
            (metrics.entries.firstOrNull { it.key.name() == name }?.value?.metricValue() as? Double) ?: 0.0
        return getMetric("connection-count")
    }

}
