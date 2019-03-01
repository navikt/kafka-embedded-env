import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.11"
    id("org.jmailen.kotlinter") version "1.20.1"
    id("com.github.ben-manes.versions") version "0.20.0"
    id("org.jetbrains.dokka") version "0.9.17"
    id("signing")
    id("io.codearte.nexus-staging") version "0.20.0"
    id("de.marcphilipp.nexus-publish") version "0.1.1"
}

repositories {
    maven("http://packages.confluent.io/maven")
    maven("https://dl.bintray.com/kotlin/ktor")
    maven("https://dl.bintray.com/spekframework/spek-dev")
    mavenCentral()
    jcenter()
}

nexusStaging {
    username = System.getenv("OSSRH_JIRA_USERNAME")
    password = System.getenv("OSSRH_JIRA_PASSWORD")
    packageGroup = "no.nav"
    stagingProfileId = "3a10cafa813c47"
}

group = "no.nav"
version = "2.0.3-SNAPSHOT"

val artifactDescription = "Simple API for running a Kafka/Confluent environment locally"
val repoUrl = "https://github.com/navikt/kafka-embedded-env.git"
val scmUrl = "scm:git:https://github.com/navikt/kafka-embedded-env.git"

val kotlinVersion = "1.3.11"

val zookeeperVersion = "3.4.13"
val kafkaVersion = "2.0.1"
val confluentVersion = "5.0.0"

val commonsVersion = "1.3.2"

val kluentVersion = "1.46"
val spekVersion = "2.0.0-rc.1"
val ktorVersion = "1.1.1"
val slf4jVersion = "1.7.5"
val logbackVersion = "1.2.3"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")
    implementation("org.apache.commons:commons-io:$commonsVersion")
    implementation ("org.apache.kafka:kafka_2.12:$kafkaVersion") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    implementation ("org.apache.kafka:kafka-streams:$kafkaVersion") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
    implementation ("io.confluent:kafka-schema-registry:$confluentVersion") {
        exclude(group = "org.apache.kafka")
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
    testImplementation("io.confluent:kafka-streams-avro-serde:$confluentVersion")
    testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
    testImplementation("org.amshove.kluent:kluent:$kluentVersion")
    testImplementation("io.ktor:ktor-client-core:$ktorVersion")
    testImplementation("io.ktor:ktor-client-apache:$ktorVersion")
    testImplementation ("org.spekframework.spek2:spek-dsl-jvm:$spekVersion")  {
        exclude(group = "org.jetbrains.kotlin")
    }
    testRuntimeOnly ("org.spekframework.spek2:spek-runner-junit5:$spekVersion") {
        exclude(group = "org.jetbrains.kotlin")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks {
    withType<KotlinCompile> {
        kotlinOptions.jvmTarget = "1.8"
    }
    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}

val dokka = tasks.withType<DokkaTask> {
    outputFormat = "html"
    outputDirectory = "$buildDir/javadoc"
}

val sourcesJar by tasks.registering(Jar::class) {
    classifier = "sources"
    from(sourceSets["main"].allSource)
}

val javadocJar by tasks.registering(Jar::class) {
    dependsOn(dokka)
    classifier = "javadoc"
    from(buildDir.resolve("javadoc"))
}

artifacts {
    add("archives", sourcesJar)
    add("archives", javadocJar)
}

configure<PublishingExtension> {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            artifact(sourcesJar.get())
            artifact(javadocJar.get())

            pom {
                name.set(project.name)
                description.set(artifactDescription)
                url.set(repoUrl)
                withXml {
                    asNode().appendNode("packaging", "jar")
                }
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                        distribution.set("repo")
                    }
                }
                developers {
                    developer {
                        organization.set("NAV (Arbeids- og velferdsdirektoratet) - The Norwegian Labour and Welfare Administration")
                        organizationUrl.set("https://www.nav.no")
                    }
                }
                scm {
                    connection.set(scmUrl)
                    developerConnection.set(scmUrl)
                    url.set(repoUrl)
                }
            }
        }
    }
}

ext["signing.gnupg.keyName"] = System.getenv("GPG_KEY_NAME")
ext["signing.gnupg.passphrase"] = System.getenv("GPG_PASSPHRASE")
ext["signing.gnupg.useLegacyGpg"] = true

configure<SigningExtension> {
    useGpgCmd()
    sign((extensions.getByName("publishing") as PublishingExtension).publications["mavenJava"])
}
