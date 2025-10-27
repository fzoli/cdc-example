package com.example.cdc.repository.message

import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Repository
import java.util.UUID

@Repository
interface MessageRepository : CrudRepository<Message, UUID>
