package com.example.cdc.service.message

import java.util.UUID

class MessageNotFoundException(val id: UUID) : RuntimeException("Message not found for id $id")
