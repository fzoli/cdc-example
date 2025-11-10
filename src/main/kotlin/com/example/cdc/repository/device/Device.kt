package com.example.cdc.repository.device

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

@Document("devices")
data class Device(
    @Id val id: String? = null,
    val name: String,
    val status: Status,
    val lastSeen: Instant,
)

enum class Status {
    ONLINE,
    OFFLINE,
}
