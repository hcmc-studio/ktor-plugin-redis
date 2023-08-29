val project_version: String by project
val jdk_version: String by project
val kotlinx_coroutines_version: String by project
val kotlinx_datetime_version: String by project
val kotlinx_serialization_version: String by project
val hcmc_extension_version: String by project
val ktor_version: String by project
val lettuce_version: String by project
val commons_pool2_version: String by project

plugins {
    kotlin("jvm")
    id("maven-publish")
}

group = "studio.hcmc"
version = project_version

repositories {
    mavenCentral()
    mavenLocal()
    maven { setUrl("https://jitpack.io") }
}

java {
    withSourcesJar()
}

kotlin {
    jvmToolchain(jdk_version.toInt())
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            groupId = "studio.hcmc"
            artifactId = project.name
            version = project_version
            from(components["java"])
        }
        create<MavenPublication>("jitpack") {
            groupId = "com.github.hcmc-studio"
            artifactId = project.name
            version = "$project_version-release"
            from(components["java"])
        }
    }
}

dependencies {
    implementation("com.github.hcmc-studio:kotlin-format-extension:$hcmc_extension_version")
    implementation("com.github.hcmc-studio:kotlin-protocol-extension:$hcmc_extension_version")
    implementation("com.github.hcmc-studio:ktor-plugin-content-negotiation:$hcmc_extension_version")
    implementation("com.github.hcmc-studio:ktor-serialization-extension:$hcmc_extension_version")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinx_coroutines_version")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:$kotlinx_coroutines_version")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime-jvm:$kotlinx_datetime_version")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-core-jvm:$kotlinx_serialization_version")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json-jvm:$kotlinx_serialization_version")

    implementation("io.ktor:ktor-server-core-jvm:$ktor_version")

    implementation("io.lettuce:lettuce-core:$lettuce_version")
    implementation("org.apache.commons:commons-pool2:$commons_pool2_version")

}