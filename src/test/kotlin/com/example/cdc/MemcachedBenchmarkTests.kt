package com.example.cdc

import net.spy.memcached.MemcachedClient
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.roundToInt
import kotlin.test.assertTrue

@Testcontainers
class MemcachedBenchmarkTests {

    companion object {
        private val image = DockerImageName.parse("memcached:latest")

        @JvmStatic
        @Container
        val memcached: GenericContainer<*> = GenericContainer(image)
            .withExposedPorts(11211)
    }

    @Test
    fun `memcached bytearray performance benchmark`() {
        val client = MemcachedClient(
            InetSocketAddress(memcached.host, memcached.getMappedPort(11211))
        )

        try {
            val valueSize = 1024 // 1 KB
            val value = ByteArray(valueSize) { (it % 251).toByte() }

            // Warmup phase
            repeat(5_000) { i ->
                val k = "w$i"
                client.set(k, 60, value)
                client.get(k)
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
                        val rnd = ThreadLocalRandom.current()
                        repeat(opsPerThread) { i ->
                            val key = "k-$t-$i"
                            client.set(key, 60, value)
                            val t0 = if (i % recordEvery == 0) System.nanoTime() else 0L
                            client.get(key)
                            if (t0 != 0L) {
                                val dt = System.nanoTime() - t0
                                synchronized(latencies) { latencies.add(dt) }
                            }
                            rnd.nextInt(3)
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
                val idx = ((p / 100.0) * (arr.size - 1)).roundToInt()
                return arr[idx] / 1_000_000.0 // ms
            }

            println("Memcached byte[] benchmark: threads=$threads value=${valueSize}B totalOps=$totalOps elapsed=${"%.2f".format(elapsedSec)}s throughput=${"%.0f".format(opsPerSec)} ops/sec (set+get)")
            println("Latency ms: p50=${"%.3f".format(pct(50.0))} p95=${"%.3f".format(pct(95.0))} p99=${"%.3f".format(pct(99.0))}")

            assertTrue(opsPerSec > 0)
        } finally {
            client.shutdown()
        }
    }
}
