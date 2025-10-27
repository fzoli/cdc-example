package com.example.cdc.event.message

import com.example.cdc.repository.message.Message

data class MessageUpsertEvent(val message: Message)
