import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.springframework.boot") version "3.4.0"
    id("io.spring.dependency-management") version "1.1.6"
    kotlin("jvm") version "2.1.0"
    kotlin("plugin.spring") version "2.1.0"
    kotlin("plugin.serialization") version "2.1.0"
}

group = "com.fraktalio.example"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_17

configurations {
    all {
        // We use Kotlin Serialization so no need for Jackson and kotlin-reflect
        exclude(module = "spring-boot-starter-json")
    }
    compileOnly {
        extendsFrom(configurations.annotationProcessor.get())
    }
}

repositories {
    mavenCentral()
    maven("https://s01.oss.sonatype.org/content/repositories/snapshots/")
}

extra["testcontainersVersion"] = "1.20.4"
extra["fmodelVersion"] = "3.6.0"
extra["kotlinxSerializationJson"] = "1.7.3"
extra["kotlinxCollectionsImmutable"] = "0.3.8"
extra["kotlinLogging"] = "3.0.5"
extra["kotlinxCoroutinesTest"] = "1.9.0"

dependencies {
    implementation("com.fraktalio.fmodel:domain:${property("fmodelVersion")}")
    implementation("com.fraktalio.fmodel:application-vanilla:${property("fmodelVersion")}")
    implementation("com.fraktalio.fmodel:application-arrow:${property("fmodelVersion")}")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-rsocket")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:${property("kotlinxSerializationJson")}")
    implementation("org.jetbrains.kotlinx:kotlinx-collections-immutable-jvm:${property("kotlinxCollectionsImmutable")}")
    implementation("org.postgresql:r2dbc-postgresql")
    implementation("io.github.microutils:kotlin-logging-jvm:${property("kotlinLogging")}")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:${property("kotlinxCoroutinesTest")}")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:r2dbc")
    testImplementation("io.projectreactor:reactor-test")

}

dependencyManagement {
    imports {
        mavenBom("org.testcontainers:testcontainers-bom:${property("testcontainersVersion")}")
    }
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "17"
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
