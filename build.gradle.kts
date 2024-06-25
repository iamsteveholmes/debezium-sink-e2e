plugins {
    application
    kotlin("jvm")
    alias(libs.plugins.test.logger)
}

group = "com.test"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Use the Kotlin JUnit 5 integration.
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")

    // Use the JUnit 5 integration.
    testImplementation(libs.junit.jupiter.engine)
    testImplementation(libs.debezium.testcontainers)
    testImplementation(libs.debezium.jdbc)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.oracle)
    testImplementation(libs.kafka.clients)
    testImplementation(libs.logback.core)
    testImplementation(libs.logback.classic)
    testImplementation(libs.slf4j)
    testImplementation(libs.kotlin.coroutines)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    implementation(kotlin("stdlib-jdk8"))
}
java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()

    testLogging {
        showStandardStreams = true
        events("standardOut", "started", "passed", "skipped", "failed")
    }
}
