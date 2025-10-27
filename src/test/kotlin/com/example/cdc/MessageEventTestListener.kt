package com.example.cdc

import com.example.cdc.event.message.MessageDeleteEvent
import com.example.cdc.event.message.MessageUpsertEvent
import com.example.cdc.repository.message.Message
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component
import java.util.Collections
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Component
class MessageEventTestListener {

    private val messages: MutableMap<UUID, Message> = ConcurrentHashMap()
    private val deletedMessageIds: MutableSet<UUID> = Collections.synchronizedSet(HashSet())

    @EventListener
    fun onMessageUpsertEvent(event: MessageUpsertEvent) {
        val id = event.message.id!!
        messages[id] = event.message
        deletedMessageIds.remove(id)
    }

    @EventListener
    fun onMessageDeleteEvent(event: MessageDeleteEvent) {
        messages.remove(event.id)
        deletedMessageIds.add(event.id)
    }

    fun awaitMessageById(id: UUID): Message? {
        var counter = 0
        while (++counter <= 10) {
            val message = findMessageById(id)
            if (message == null) {
                Thread.sleep(100)
                continue
            }
            return message
        }
        return null
    }

    fun awaitMessageDeleteById(id: UUID): Boolean {
        var counter = 0
        while (++counter <= 10) {
            if (deletedMessageIds.contains(id)) {
                return true
            }
            Thread.sleep(100)
        }
        return false
    }

    fun findMessageById(id: UUID): Message? {
        return messages[id]
    }

    fun removeMessageById(id: UUID): Boolean {
        return messages.remove(id) != null
    }

}
