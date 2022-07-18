val kotlin_version: String by project
val logback_version: String by project
val ktor_version = "1.6.8"
val ratis_version = "2.2.0"

plugins {
    application
    kotlin("jvm") version "1.6.21"
    kotlin("plugin.serialization") version "1.6.10"
}

group = "com.example"
version = "0.0.1"
application {
    mainClass.set("com.example.ApplicationKt")

    val isDevelopment: Boolean = project.ext.has("development")
    applicationDefaultJvmArgs = listOf("-Dio.ktor.development=$isDevelopment")
}

repositories {
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/ktor/eap") }
}

dependencies {
    //ktor
    implementation("io.ktor:ktor-server:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-jackson:$ktor_version")
    implementation("ch.qos.logback:logback-classic:$logback_version")
    implementation("io.ktor:ktor-client-core:$ktor_version")
    implementation("io.ktor:ktor-client-okhttp:$ktor_version")
    implementation("io.ktor:ktor-client-jackson:$ktor_version")

    // object mapper
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.2")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.0.4")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.0.4")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")

    implementation("org.apache.ratis:ratis:$ratis_version")
    implementation("org.apache.ratis:ratis-proto:$ratis_version")
    implementation("org.apache.ratis:ratis-grpc:$ratis_version")
    implementation("org.apache.ratis:ratis-common:$ratis_version")
    implementation("org.apache.ratis:ratis-server-api:$ratis_version")
    implementation("org.apache.ratis:ratis-tools:$ratis_version")
    implementation("org.apache.ratis:ratis-client:$ratis_version")
    implementation("org.apache.ratis:ratis-thirdparty-misc:0.7.0")

    testImplementation(platform("org.junit:junit-bom:5.8.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.0")
    testImplementation("io.mockk:mockk:1.12.4")
    testImplementation("org.awaitility:awaitility:4.2.0")

    // wiremock
    testImplementation("com.github.tomakehurst:wiremock-jre8:2.33.2")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    kotlinOptions.jvmTarget = "11"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "com.example.ApplicationKt"
    }
}

tasks {
    "test"(Test::class) {
        filter {
            excludeTestsMatching("com.example.api.SinglePeersetIntegrationTest")
            excludeTestsMatching("com.example.api.MultiplePeersetSpec")
            excludeTestsMatching("com.example.consensus.ConsensusSpec")
        }
    }
}

val singlePeersetIntegrationTest = sourceSets.create("singlePeersetIntegrationTest")
tasks.register<Test>("singlePeersetIntegrationTest") {
    group = "verification"

    shouldRunAfter(tasks.test)
    useJUnitPlatform()

    filter {
        includeTestsMatching("com.example.api.SinglePeersetIntegrationTest")
        includeTestsMatching("com.example.consensus.ConsensusSpec")
    }
}

val integrationTest = sourceSets.create("integrationTest")
tasks.register<Test>("integrationTest") {
    group = "verification"

    shouldRunAfter(tasks.test)
    useJUnitPlatform()

    filter {
        includeTestsMatching("com.example.api.MultiplePeersetSpec")
    }
}
