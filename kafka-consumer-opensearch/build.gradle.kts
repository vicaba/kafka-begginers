plugins {
    id("java")
}

group = "com.vicaba"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:3.6.1")
    implementation("org.slf4j:slf4j-api:2.0.10")
    implementation("org.slf4j:slf4j-simple:2.0.10")
    implementation("org.opensearch.client:opensearch-rest-high-level-client:2.11.1")
    implementation("com.google.code.gson:gson:2.10.1")
}

tasks.test {
    useJUnitPlatform()
}