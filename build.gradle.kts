val kotlin_version: String by project
val logback_version: String by project
val ktor_version = "1.6.4"
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
    implementation("io.ktor:ktor-serialization:$ktor_version")
    implementation("ch.qos.logback:logback-classic:$logback_version")
    implementation("io.insert-koin:koin-ktor:3.1.6")

    // object mapper
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.13.2")

    // config reading
    implementation("com.sksamuel.hoplite:hoplite-core:2.0.4")
    implementation("com.sksamuel.hoplite:hoplite-hocon:2.0.4")

    // ratis - consensus library
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
