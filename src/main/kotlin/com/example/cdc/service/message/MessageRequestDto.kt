package com.example.cdc.service.message

import java.util.UUID

data class MessageRequestDto(
    val id: UUID? = null,
    val username: String,
    val message: String,
)
