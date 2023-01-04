# KHOOK

> khook /kæ'hʊk/
> 
> Pronounced `ka` as in `cache`, `catch` and `Kafka`, plus `hʊk` as in `webhook` and it's original word `hook`.
> 
> It forms from 2 words: Kafka and webHOOK.

## What is this?
**khook* is a basic Kafka Consumer that translate all your Kafka message into [CloudEvents](https://cloudevents.io/).
By joining force from the two world, we can make our event stream more consistent, accessible, and portable 
(where we get from CloudEvents<sup>1</sup>), and at the same time make our stream more performant and reliable (by using Kafka<sup>2</sup>).

<sup>1</sup> 

<sup>2</sup>

## Other alternatives

* [KNative Eventing](https://github.com/knative-sandbox/eventing-kafka-broker) will route events from event producers to event consumers, known as sinks, that receive events. Sinks can also be configured to respond to HTTP requests by sending a response event.
  KNative Eventing [support Kafka and RabbitMQ as Broker and you can filter the message to specific Sink.](https://github.com/knative/docs/blob/2498912cd14669b25bb37dc848fab2644c612f19/docs/snippets/about-brokers.md#L6)
  But, since KNative is build on top Kubernetes, you'll only see the step as Kubernetes CRDs installation (where you see many of `kubectl apply` command).
  `khook`, in the other hand, is a binary that can be installed in any environment, with or without Kubernetes. It just like a regular Backend application
  with REST API interface, and wide-range of users (from Backend Developer, DevOps or even QA) should easily understand about how to install, configure and use it.

## Getting started

