package com.example.cdc.service.message

import com.example.cdc.repository.message.Message
import com.example.cdc.repository.message.MessageRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Isolation
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Instant
import java.util.UUID

@Service
class MessageService(
    private val clock: Clock,
    private val messageRepository: MessageRepository,
) {

    @Transactional(isolation = Isolation.SERIALIZABLE, readOnly = false, timeout = 3)
    fun upsertMessage(request: MessageRequestDto): Message { // the return type should be a DTO too, but it's not necessary for this example
        val now = Instant.now(clock)
        val createTime = if (request.id != null) {
            messageRepository.findById(request.id).orElseThrow { MessageNotFoundException(request.id) }.createTime
        } else {
            now
        }
        val result = messageRepository.save(Message(
            id = request.id,
            createTime = createTime,
            updateTime = now,
            username = request.username,
            message = request.message,
        ))
        return messageRepository.findById(result.id!!).orElseThrow() // read back to return the correct time stamps
    }

    @Transactional(isolation = Isolation.SERIALIZABLE, readOnly = false, timeout = 3)
    fun deleteMessage(messageId: UUID) {
        messageRepository.deleteById(messageId)
    }

}
