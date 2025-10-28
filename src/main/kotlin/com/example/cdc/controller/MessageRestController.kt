package com.example.cdc.controller

import com.example.cdc.repository.message.Message
import com.example.cdc.service.message.MessageRequestDto
import com.example.cdc.service.message.MessageService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
@RequestMapping("/api/messages")
class MessageRestController(private val messageService: MessageService) {

    @PostMapping
    fun upsertMessage(@RequestBody request: MessageRequestDto): Message {
        return messageService.upsertMessage(request)
    }

    @DeleteMapping("{messageId}")
    fun deleteMessage(@PathVariable messageId: UUID): ResponseEntity<Unit> {
        messageService.deleteMessage(messageId)
        return ResponseEntity.noContent().build()
    }

}
