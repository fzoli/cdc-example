package com.example.cdc.consumer

import com.example.cdc.event.message.MessageDeleteEvent
import com.example.cdc.event.message.MessageUpsertEvent
import com.example.cdc.repository.message.Message
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID

@Service
class MessageConsumer(
    private val objectMapper: ObjectMapper,
    private val eventPublisher: ApplicationEventPublisher,
) {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    @KafkaListener(topics = ["messages.public.messages"])
    fun consume(message: ConsumerRecord<String, String>) {
        val typeRef = object : TypeReference<DebeziumWrapper<MessageRecord>>() {}
        val wrapper: DebeziumWrapper<MessageRecord> = objectMapper.readValue(message.value(), typeRef)
        val record = wrapper.after
        if (record == null) {
            val key = objectMapper.readValue(message.key(), MessageId::class.java)
            logger.info("Received message delete event for message id: {}", key.id)
            eventPublisher.publishEvent(
                MessageDeleteEvent(key.id)
            )
        } else {
            logger.info("Received message upsert event for message id: {}", record.id)
            eventPublisher.publishEvent(
                MessageUpsertEvent(
                    Message(
                        id = record.id,
                        createTime = record.createTime,
                        updateTime = record.updateTime,
                        message = record.message,
                        username = record.username,
                    )
                )
            )
        }
    }

    private data class DebeziumWrapper<T>(
        val after: T?
    )

    private data class MessageRecord(
        val id: UUID,
        @field:JsonProperty("create_time")
        val createTime: Instant,
        @field:JsonProperty("update_time")
        val updateTime: Instant,
        val message: String,
        val username: String,
    )

    private data class MessageId(
        val id: UUID,
    )
}
