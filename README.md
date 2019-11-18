= Sample Confluent Cloud Metrics Firehose to Prometheus Application

This sample application has 2 build-in execution models, the `PrometheusPuller` and the `PrometheusPusher`.
Together each of these application builds will either leverage the Prometheus Push Gateway, `PrometheusPusher`,
or will leverage the traditional pull based model, `PrometheusPuller`.

== Building

Each application version has a given Maven Profile to build Uber Jars with Manafest with MainClass definitions.

**Prometheus Push Gateway**

```
mvn package -PPromPusher
```

**Prometheus Pull Model**

```
mvn package -PPromPuller
```

The Maven build also supports building a Docker Container application.
This application leverages the `confluentinc/cp-base` base image.

**Prometheus Push Gateway with Docker**

```
mvn package -PPromPusher,Docker
```

**Prometheus Pull Model with Docker**

```
mvn package -PPromPuller,Docker
```