package com.example.cdc

import com.example.cdc.service.message.MessageRequestDto
import com.example.cdc.service.message.MessageService
import org.apache.kafka.clients.consumer.ConsumerRecord
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
        val note = Vector<Pair<Instant, Long>>()
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val latch = CountDownLatch(10)
        repeat(10) {
            executor.submit {
                randomAccess(note)
                latch.countDown()
            }
        }
        latch.await()
        assertThat(note.size).isEqualTo(40)
        for (i in 0 until note.size - 1) {
            val current = note[i].first
            val next = note[i + 1].first
            assertThat(next).isAfter(current)
        }
    }

    private fun randomAccess(note: Vector<Pair<Instant, Long>>) {
        val props = Properties().apply {
            this["bootstrap.servers"] = "${kafka.host}:${kafka.getMappedPort(9092)}"
            this["enable.auto.commit"] = "false"
            this["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            this["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
            this["fetch.max.wait.ms"] = "100"
            this["max.poll.records"] = "1"
            this["default.api.timeout.ms"] = "3000"
        }

        val tp = TopicPartition("messages.public.messages", 0)
        val records = KafkaConsumer<String, String>(props).use { consumer ->
            consumer.assign(listOf(tp))
            fun read(offset: Long): ConsumerRecord<String, String>? {
                consumer.seek(tp, offset)
                val records = consumer.poll(Duration.ofMillis(100))
                note.add(Pair(Instant.now(), offset))
                return records.firstOrNull()
            }
            listOf(read(0), read(1), read(2), read(0))
        }
        assertThat(records[0]?.offset()).isEqualTo(0)
        assertThat(records[1]?.offset()).isEqualTo(1)
        assertThat(records[2]?.offset()).isEqualTo(2)
        assertThat(records[3]?.offset()).isEqualTo(0)
    }

}
