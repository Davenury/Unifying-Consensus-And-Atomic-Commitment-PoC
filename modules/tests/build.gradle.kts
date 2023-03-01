import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val ktor_version = "1.6.8"
val logback_version: String by project

plugins {
    id("application")
    kotlin("jvm")
}

application {
    mainClass.set("com.github.davenury.tests.TestNotificationServiceKt")
}

sourceSets {
    main {
        java.srcDir("src/main/kotlin")
    }
}
dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.1")

    implementation("io.ktor:ktor-server:$ktor_version")
    implementation("io.ktor:ktor-server-netty:$ktor_version")
    implementation("io.ktor:ktor-jackson:$ktor_version")

    implementation("io.ktor:ktor-client-core:$ktor_version")
    implementation("io.ktor:ktor-client-okhttp:$ktor_version")
    implementation("io.ktor:ktor-client-jackson:$ktor_version")

    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("ch.qos.logback:logback-classic:$logback_version")
    implementation("com.github.loki4j:loki-logback-appender:1.4.0")

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.2")
    implementation("io.ktor:ktor-metrics-micrometer:$ktor_version")
    implementation("io.prometheus:simpleclient_pushgateway:0.16.0")

    implementation("com.sksamuel.hoplite:hoplite-core:2.0.4")

    implementation(project(":modules:common"))

    testImplementation(platform("org.junit:junit-bom:5.8.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.0")
    testImplementation("io.mockk:mockk:1.12.4")
}
repositories {
    mavenCentral()
}
val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "11"
}
val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "11"
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<Jar> {
    manifest {
        attributes["Main-Class"] = "com.github.davenury.tests.TestNotificationServiceKt"
    }
}
