import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val ktor_version = "1.6.8"

plugins {
    id("application")
    kotlin("jvm")
}

sourceSets {
    main {
        java.srcDir("src/main/kotlin")
    }
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.2")

    implementation("io.ktor:ktor-client-core:$ktor_version")
    implementation("io.ktor:ktor-client-okhttp:$ktor_version")
    implementation("io.ktor:ktor-client-jackson:$ktor_version")

    implementation("io.micrometer:micrometer-registry-prometheus:1.9.2")
    implementation("io.ktor:ktor-metrics-micrometer:$ktor_version")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.6.5")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.6.5")

    implementation(kotlin("stdlib-jdk8"))

    implementation("redis.clients:jedis:4.3.0")

    testImplementation(platform("org.junit:junit-bom:5.8.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.strikt:strikt-core:0.34.0")
    testImplementation("org.testcontainers:testcontainers:1.17.6")
    testImplementation("org.testcontainers:junit-jupiter:1.17.6")
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
