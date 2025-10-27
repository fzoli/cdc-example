package com.example.cdc.controller

import com.example.cdc.repository.message.Message
import com.example.cdc.service.message.MessageRequestDto
import com.example.cdc.service.message.MessageService
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/messages")
class MessageRestController(private val messageService: MessageService) {

    @PostMapping
    fun upsertMessage(@RequestBody request: MessageRequestDto): Message {
        return messageService.upsertMessage(request)
    }

}
