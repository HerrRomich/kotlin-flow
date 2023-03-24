package io.github.herromich.kotlinflow

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication(scanBasePackages = arrayOf("io.github.herromich.kotlinflow"))
class Launcher

fun main(args: Array<String>) {
    runApplication<Launcher>(*args)
}
