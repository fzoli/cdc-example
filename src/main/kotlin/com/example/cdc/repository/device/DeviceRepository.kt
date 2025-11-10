package com.example.cdc.repository.device

import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.stereotype.Repository

@Repository
interface DeviceRepository : MongoRepository<Device, String> {
    fun findByName(name: String): Device?
    fun findByStatus(status: Status): List<Device>
}
