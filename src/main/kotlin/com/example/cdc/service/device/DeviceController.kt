package com.example.cdc.service.device

import com.example.cdc.repository.device.Device
import com.example.cdc.repository.device.DeviceRepository
import com.example.cdc.repository.device.Status
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

@RestController
class DeviceController(private val repository: DeviceRepository) {

    @PostMapping("/devices")
    fun createDevice(): Device {
        val device = Device(fullName = "A", status = Status.ONLINE, lastSeen = Instant.now())
        repository.save(device)
        return device
    }

}