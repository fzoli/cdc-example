package com.example.cdc

import com.example.cdc.repository.device.Device
import com.example.cdc.repository.device.DeviceRepository
import com.example.cdc.repository.device.Status
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.event.RecordApplicationEvents
import java.time.Instant

@Import(TestcontainersConfiguration::class)
@SpringBootTest
@RecordApplicationEvents
class DeviceRepoTest @Autowired constructor(
    private val repository: DeviceRepository
) {

    @Test
    fun save() {
        val device = Device(fullName = "A", status = Status.ONLINE, lastSeen = Instant.now())
        repository.save(device)
        Assertions.assertEquals(1, repository.findAll().size)
    }

}
