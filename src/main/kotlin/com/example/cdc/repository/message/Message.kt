package com.example.cdc.repository.message

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant
import java.util.UUID

@Table("messages")
data class Message(
    @Id
    @Column("id")
    val id: UUID? = null,
    @Column("create_time")
    val createTime: Instant,
    @Column("update_time")
    val updateTime: Instant,
    @Column("username")
    val username: String,
    @Column("message")
    val message: String,
)
