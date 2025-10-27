package com.example.cdc.configuration

import org.springframework.boot.SpringApplication
import org.springframework.boot.env.EnvironmentPostProcessor
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource
import java.net.InetAddress

class HostnamePropertySourceInitializer : EnvironmentPostProcessor {
    override fun postProcessEnvironment(environment: ConfigurableEnvironment, application: SpringApplication) {
        val hostname = InetAddress.getLocalHost().hostName
        environment.propertySources.addFirst(
            MapPropertySource("hostname", mapOf("system.hostname" to hostname))
        )
    }
}
