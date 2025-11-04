package com.example.cdc

import org.junit.jupiter.api.Test
import org.springframework.cache.Cache
import org.springframework.data.redis.cache.RedisCacheConfiguration
import org.springframework.data.redis.cache.RedisCacheManager
import org.springframework.data.redis.connection.MessageListener
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.RedisSerializer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Testcontainers
class RedisPubSubTests {

    companion object {
        private val image = DockerImageName.parse("eqalpha/keydb:latest")
        private val network = Network.newNetwork()

        @JvmStatic
        @Container
        val keydb1: GenericContainer<*> = GenericContainer(image)
            .withExposedPorts(6379)
            .withNetwork(network)
            .withNetworkAliases("node1")
            .withCommand(
                "keydb-server",
                "--port", "6379",
                "--multi-master", "yes",
                "--active-replica", "yes",
                "--replicaof", "node2", "6379",
            )
        @JvmStatic
        @Container
        val keydb2: GenericContainer<*> = GenericContainer(image)
            .withExposedPorts(6379)
            .withNetwork(network)
            .withNetworkAliases("node2")
            .withCommand(
                "keydb-server",
                "--port", "6379",
                "--multi-master", "yes",
                "--active-replica", "yes",
                "--replicaof", "node1", "6379",
            )
    }

    @Test
    fun `pubsub should deliver message`() {
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        val factory = LettuceConnectionFactory(keydb1.host, keydb1.getMappedPort(6379)).apply { afterPropertiesSet() }

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

    @Test
    fun `cache put-get and ttl`() {
        val factory = LettuceConnectionFactory(keydb1.host, keydb1.getMappedPort(6379)).apply { afterPropertiesSet() }

        try {
            val config = RedisCacheConfiguration.defaultCacheConfig()
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(RedisSerializer.byteArray()))
                .entryTtl(Duration.ofMillis(500))
            val cacheManager = RedisCacheManager.builder(factory)
                .cacheDefaults(config)
                .build()

            val cache = cacheManager.getCache("test-cache")
            requireNotNull(cache) { "Cache should not be null" }

            cache.put("k1", "v1".toByteArray())
            val value: ByteArray? = cache.get("k1", ByteArray::class.java)
            assertEquals("v1", String(value!!))

            Thread.sleep(800)
            val expired = cache.get("k1")
            assertTrue(expired == null || expired.get() == null, "Expected cache entry to expire")
        } finally {
            factory.destroy()
        }
    }

    @Test
    fun `cache bytearray performance benchmark`() {
        val factory1 = LettuceConnectionFactory(keydb2.host, keydb2.getMappedPort(6379)).apply { afterPropertiesSet() }
        val factory2 = LettuceConnectionFactory(keydb2.host, keydb2.getMappedPort(6379)).apply { afterPropertiesSet() }
        val rr = RoundRobinCache(listOf(factory1, factory2))
        try {
            val valueSize = 1024 // 1 KB
            val value = ByteArray(valueSize) { (it % 251).toByte() }

            // Warmup
            repeat(5_000) { i ->
                val k = "w$i"
                val cache = rr.next()
                cache.put(k, value)
                cache.get(k, ByteArray::class.java)
            }

            val threads = maxOf(2, Runtime.getRuntime().availableProcessors())
            val opsPerThread = 10_000
            val totalOps = threads * opsPerThread

            val latch = CountDownLatch(threads)
            val latencies = mutableListOf<Long>()
            val recordEvery = 50 // sample every Nth op per thread for latency

            val start = System.nanoTime()
            repeat(threads) { t ->
                Thread.startVirtualThread {
                    try {
                        val cache = rr.next()
                        val rnd = ThreadLocalRandom.current()
                        var c = 0
                        repeat(opsPerThread) { i ->
                            val k = "k-$t-${i}"
                            cache.put(k, value)
                            val t0 = if (i % recordEvery == 0) System.nanoTime() else 0L
                            cache.get(k, ByteArray::class.java)
                            if (t0 != 0L) {
                                val dt = System.nanoTime() - t0
                                synchronized(latencies) { latencies.add(dt) }
                            }
                            c += rnd.nextInt(3)
                        }
                    } finally {
                        latch.countDown()
                    }
                }
            }

            latch.await(30, TimeUnit.SECONDS)
            val elapsedNanos = System.nanoTime() - start
            val elapsedSec = elapsedNanos / 1_000_000_000.0
            val opsPerSec = totalOps * 2 / elapsedSec // put + get per op

            fun pct(p: Double): Double {
                if (latencies.isEmpty()) return Double.NaN
                val arr = latencies.toMutableList().sorted()
                val idx = ((p / 100.0) * (arr.size - 1)).toInt()
                return arr[idx] / 1_000_000.0 // ms
            }

            println("Cache byte[] benchmark: threads=$threads value=${valueSize}B totalOps=$totalOps elapsed=${"%.2f".format(elapsedSec)}s throughput=${"%.0f".format(opsPerSec)} ops/sec (put+get)")
            println("Latency ms: p50=${"%.3f".format(pct(50.0))} p95=${"%.3f".format(pct(95.0))} p99=${"%.3f".format(pct(99.0))}")
        } finally {
            rr.close()
        }
    }
}

class RoundRobinCache(
    private val factories: List<LettuceConnectionFactory>
) : AutoCloseable {

    private val counter = AtomicInteger(0)

    private val caches = factories.map { factory ->
        val config = RedisCacheConfiguration.defaultCacheConfig()
            .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(RedisSerializer.byteArray()))
            .entryTtl(Duration.ofSeconds(60))
        val cacheManager = RedisCacheManager.builder(factory)
            .cacheDefaults(config)
            .build()
        requireNotNull(cacheManager.getCache("bench-cache"))
    }

    fun next(): Cache {
        val i = counter.getAndIncrement()
        return caches[i % factories.size]
    }

    override fun close() {
        factories.forEach { it.destroy() }
    }

}
