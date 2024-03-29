val kotlin_version: String by project
val logback_version: String by project
val ktor_version = "1.6.8"
val ratis_version = "2.2.0"
val slf4j_version = "2.0.3"

plugins {
    application
    kotlin("jvm") version "1.6.21"
    kotlin("plugin.serialization") version "1.6.10"
    id("com.adarshr.test-logger") version "3.2.0"
}

group = "com.example"
version = "0.0.1"
application {
    mainClass.set("com.github.davenury.ucac.ApplicationUcacKt")

    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/ktor/eap") }
}

dependencies {

    implementation(project(":modules:common"))

    implementation("org.slf4j:slf4j-api:$slf4j_version")
    implementation("ch.qos.logback:logback-classic:$logback_version")

    //ktor
    implementation("io.ktor:ktor-server:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-jackson:$ktor_version")
    implementation("io.ktor:ktor-client-core:$ktor_version")
    implementation("io.ktor:ktor-client-okhttp:$ktor_version")
    implementation("io.ktor:ktor-client-jackson:$ktor_version")

    // object mapper
    implementation("com.fasterxml.jackson.core:jackson-databind:2.14.0")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.0")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.6.5")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.6.5")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-slf4j:1.6.4")

    // metrics
    implementation("io.micrometer:micrometer-registry-prometheus:1.10.0")
    implementation("io.ktor:ktor-metrics-micrometer:$ktor_version")

    // traces
    implementation("io.jaegertracing:jaeger-client:1.8.1")
    implementation("com.zopa:ktor-opentracing:0.3.6")

    implementation("org.apache.ratis:ratis:$ratis_version")
    implementation("org.apache.ratis:ratis-proto:$ratis_version")
    implementation("org.apache.ratis:ratis-grpc:$ratis_version")
    implementation("org.apache.ratis:ratis-common:$ratis_version")
    implementation("org.apache.ratis:ratis-server-api:$ratis_version")
    implementation("org.apache.ratis:ratis-tools:$ratis_version")
    implementation("org.apache.ratis:ratis-client:$ratis_version")
    implementation("org.apache.ratis:ratis-thirdparty-misc:0.7.0")

    implementation("com.github.loki4j:loki-logback-appender:1.4.0")
    testImplementation(platform("org.junit:junit-bom:5.9.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("io.mockk:mockk:1.13.2")
    testImplementation("org.awaitility:awaitility:4.2.0")
    testImplementation("org.testcontainers:testcontainers:1.17.5")
    testImplementation("org.testcontainers:junit-jupiter:1.17.5")

    // for disabling AnsiConsole in tests
    testImplementation("org.fusesource.jansi:jansi:2.4.0")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions.jvmTarget = "11"
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "com.github.davenury.ucac.ApplicationKt"
    }
}
