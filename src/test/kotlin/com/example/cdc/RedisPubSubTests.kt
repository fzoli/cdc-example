package com.example.cdc

import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Testcontainers
class RedisPubSubTests {

    companion object {
        @JvmStatic
        @Container
        val redis: GenericContainer<*> = GenericContainer(DockerImageName.parse("eqalpha/keydb:latest")).withExposedPorts(6379)
    }

    @Test
    fun `pubsub should deliver message`() {
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val factory = LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply { afterPropertiesSet() }

        val template = StringRedisTemplate(factory)

        val listenerContainer = RedisMessageListenerContainer().apply {
            setConnectionFactory(factory)
            afterPropertiesSet()
            start()
        }

        val topic = ChannelTopic("test-channel")
        val latch = CountDownLatch(10)
        var received: String? = null

        val listener = MessageListener { message, _ ->
            received = String(message.body)
            latch.countDown()
        }

        listenerContainer.addMessageListener(listener, topic)

        repeat(10) {
            executor.submit {
                template.convertAndSend(topic.topic, "hello-redis")
            }
        }

        val ok = latch.await(5, TimeUnit.SECONDS)
        executor.shutdown()

        try {
            assertTrue(ok, "Expected to receive a Redis/KeyDB pub/sub message")
            assertEquals("hello-redis", received)
        } finally {
            listenerContainer.stop()
            factory.destroy()
        }
    }
}
