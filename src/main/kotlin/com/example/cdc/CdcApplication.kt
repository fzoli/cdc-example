package com.example.cdc

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class CdcApplication

fun main(args: Array<String>) {
	runApplication<CdcApplication>(*args)
}
