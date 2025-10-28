package com.example.cdc.socket

import com.example.cdc.event.message.MessageDeleteEvent
import com.example.cdc.event.message.MessageUpsertEvent
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import org.springframework.web.socket.CloseStatus
import org.springframework.web.socket.TextMessage
import org.springframework.web.socket.WebSocketSession
import org.springframework.web.socket.handler.TextWebSocketHandler
import java.time.Instant
import java.util.*

@Component
class MessageWebSocketHandler(
    private val objectMapper: ObjectMapper,
) : TextWebSocketHandler() {

    companion object {
        private val logger = LoggerFactory.getLogger(this::class.java)
    }

    private val sessions = Vector<WebSocketSession>()

    override fun afterConnectionEstablished(session: WebSocketSession) {
        sessions.add(session)
    }

    override fun afterConnectionClosed(session: WebSocketSession, status: CloseStatus) {
        sessions.remove(session)
    }

    @EventListener
    fun onMessageUpsertEvent(event: MessageUpsertEvent) {
        val wsEvent = WsEvent(
            type = EventType.UPSERT,
            id = event.message.id!!,
            content = WsMessage(
                createTime = event.message.createTime,
                updateTime = event.message.updateTime,
                username = event.message.username,
                message = event.message.message,
            )
        )
        broadcast(wsEvent)
    }

    @EventListener
    fun onMessageDeleteEvent(event: MessageDeleteEvent) {
        val wsEvent = WsEvent(type = EventType.DELETE, id = event.id, content = null)
        broadcast(wsEvent)
    }

    private fun broadcast(wsEvent: WsEvent) {
        val json = objectMapper.writeValueAsString(wsEvent)
        val message = TextMessage(json)
        sessions.forEach { session ->
            try {
                session.sendMessage(message)
            } catch (ex: Exception) {
                logger.error("Failed to send message to client", ex)
            }
        }
    }

    private data class WsEvent(
        val type: EventType,
        val id: UUID,
        val content: WsMessage?,
    )

    private enum class EventType {
        UPSERT,
        DELETE,
    }

    private data class WsMessage(
        val createTime: Instant,
        val updateTime: Instant,
        val username: String,
        val message: String,
    )

}
