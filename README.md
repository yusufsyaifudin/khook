# KHOOK

> khook /kæ'hʊk/
> 
> Pronounced `ka` as in `cache`, `catch` and `Kafka`, plus `hʊk` as in `webhook` and it's original word `hook`.
> 
> It forms from 2 words: Kafka and webHOOK.

## STATUS
This project is still in POC status. If you want to contribute, then nice! THANKS!

## What is this?
**khook** is a basic Kafka Consumer that translate all your Kafka message into [CloudEvents](https://cloudevents.io/).
By joining force from the two world, we can make our event stream more consistent, accessible, and portable 
(where we get from CloudEvents<sup>1</sup>), and at the same time make our stream more performant and reliable (by using Kafka<sup>2</sup>).

<sup>1</sup> The goal of CloudEvents specification is to agree on how we describing events so the event will be consistent. 
Also, we can move our client from one server to another server without the fear of compatibility.

<sup>2</sup> Kafka is proven to have strong performance under big load.

## Other alternatives

* [KNative Eventing](https://github.com/knative-sandbox/eventing-kafka-broker) will route events from event producers to event consumers, known as sinks, that receive events. Sinks can also be configured to respond to HTTP requests by sending a response event.
  KNative Eventing [support Kafka and RabbitMQ as Broker and you can filter the message to specific Sink.](https://github.com/knative/docs/blob/2498912cd14669b25bb37dc848fab2644c612f19/docs/snippets/about-brokers.md#L6)
  But, since KNative is build on top Kubernetes, you'll only see the step as Kubernetes CRDs installation (where you see many of `kubectl apply` command).
  `khook`, in the other hand, is a binary that can be installed in any environment, with or without Kubernetes. It just like a regular Backend application
  with REST API interface, and wide-range of users (from Backend Developer, DevOps or even QA) should easily understand about how to install, configure and use it.

* [Benthos](https://www.benthos.dev/) Benthos is a declarative data streaming service that solves a wide range of data engineering problems with simple, chained, stateless processing steps.
  Benthos support many inputs (not only Kafka) and outputs (not only HTTP). But, it doesn't have feature to pause Kafka stream (since it not limited to Kafka) and
  although it support [Streams API](https://www.benthos.dev/docs/guides/streams_mode/using_rest_api) where we can add or delete the Stream dynamically
  but, since Benthos doesn't have database we need to POST each stream into multiple Benthos deployment.
  `khook` in the other hand always **looping** the configuration from Database as source of truth, so every Deployment of `khook`
  will always **eventually** has the same stream as long as it connect to the same database.
  In future design, we may use `etcd` watcher like Kubernetes so every changes will be listened by every `khook` deployment, but for POC looping database is enough.


Comparison Table

| Feature              | Khook                                                                                                                                                                             | KNative                                                                                                                                                                                                                                          | Benthos                                                                                                                       |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| Useful when          | Your stream type is only from Kafka and sink to HTTP service. Creating CloudEvents client is as simple as creating an REST API. | You have dedicated DevOps that understand Kubernetes and want adding more stack into Kubernetes. | Your source and target stream is not limited to Kafka and HTTP.                                                               |
| Source of stream     | Kafka only                                                                                                                                                                        | Kafka, RabbitMQ, ?                                                                                                                                                                                                                               | Many [inputs](https://www.benthos.dev/docs/components/inputs/about) supported.                                                |
| Sink type            | CloudEvents (HTTP)                                                                                                                                                                | CloudEvents (HTTP)                                                                                                                                                                                                                               | Many [outputs](https://www.benthos.dev/docs/components/outputs/about) supported.                                              |
| Type of installation | As single binary with one database as source of truth: can be deployed in any server (VPS, or Kubernetes), so it easier to understand how to restart/stop or even backup program. | As [Kubernetes Custom Resource Definitions](https://github.com/knative/docs/blob/892540960d1082e38d3c1fccb73c62bb96af461f/docs/install/yaml-install/eventing/install-eventing-with-yaml.md), so you need a basic understanding about Kubernetes. | Single binary, but without database as coordinator.                                                                           |
| Processor            | Not yet supported                                                                                                                                                                 | Only [trigger filter by CloudEvents attributes](https://github.com/knative/docs/blob/2498912cd14669b25bb37dc848fab2644c612f19/docs/eventing/triggers/README.md#L65)                                                                              | Support many processors, including their own mapping language: [Bloblang](https://www.benthos.dev/docs/guides/bloblang/about) |

## Getting started

### Running

For now, the only way to run is build from source. `git clone` this project and then run `go mod download && go run main.go`


### Add new Kafka Broker Client

Ensure that your Kafka broker is ready. Change `localhost:9092` to your Kafka Broker addresses.

```shell
curl -L -X PUT 'localhost:3333/api/v1/resources' -H 'Content-Type: application/json' --data-raw '{
    "resource": {
        "apiVersion": "khook/v1",
        "kind": "KafkaBroker",
        "name": "my-connection",
        "namespace": "my-team",
        "spec": {
            "brokers": [
                "localhost:9092"
            ]
        }
    }
}'
```

### Add new consumer

```shell
curl -L -X PUT 'localhost:3333/api/v1/resources' -H 'Content-Type: application/json' --data-raw '{
    "resource": {
        "apiVersion": "khook/v1",
        "kind": "KafkaConsumer",
        "name": "my-consumer",
        "namespace": "my-team",
        "spec": {
            "selector": {
                "name": "my-connection",
                "kafka_topic": "benthos-test"
            },
            "sinkTarget": {
                "type": "cloudevents",
                "cloudevents": {
                    "url": "https://eoljmnied6huesi.m.pipedream.net",
                    "type": "payment-event"
                }
            }
        }
    }
}'
```

Change `https://example.com` to your HTTP URL or use http://pipedream.com/ for testing purpose.
Ensure that `kafka-topic-test` is ready in your Kafka broker.

### Ensure that the stream is connected

```shell
curl -L -X GET 'localhost:3333/api/v1/stats/active-consumers'
```

### Publish Event to Kafka

```shell
echo '{"foo":"bar"}' | kcat -b localhost:9092 -P -t kafka-topic-test
```

You should see a CloudEvents message in your target URL service.

