package com.example.cdc.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

@Configuration
class ClockConfiguration {
    @Bean
    fun clock(): Clock {
        return Clock.systemUTC()
    }
}