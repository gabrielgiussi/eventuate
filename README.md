[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/RBMHTechnology/eventuate?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/RBMHTechnology/eventuate.svg?branch=master)](https://travis-ci.org/RBMHTechnology/eventuate)
[![Stories in Ready](https://badge.waffle.io/rbmhtechnology/eventuate.svg?label=ready&title=Ready)](http://waffle.io/rbmhtechnology/eventuate)

Eventuate
=========

Eventuate is a toolkit for building applications composed of event-driven and event-sourced services that communicate via causally ordered event streams. Services can either be co-located on a single node or distributed up to global scale. Services can also be replicated with causal consistency and remain available for writes during network partitions. Eventuate has a [Java](http://www.oracle.com/technetwork/java/javase/overview/index.html) and [Scala](http://www.scala-lang.org/) API, is written in Scala and built on top of [Akka](http://akka.io), a toolkit for building highly concurrent, distributed, and resilient message-driven applications on the JVM. Eventuate

- provides abstractions for building stateful event-sourced services, persistent and in-memory query databases and event processing pipelines
- enables services to communicate over a reliable and partition-tolerant event bus with causal event ordering and distribution up to global scale
- supports stateful service replication with causal consistency and concurrent state updates with automated and interactive conflict resolution
- provides implementations of operation-based CRDTs as specified in [A comprehensive study of Convergent and Commutative Replicated Data Types](http://hal.upmc.fr/file/index/docid/555588/filename/techreport.pdf)
- supports the development of *always-on* applications by allowing services to be distributed across multiple availability zones (locations)
- supports the implementation of reliable business processes from event-driven and command-driven service interactions
- supports the aggregation of events from distributed services for updating query databases
- provides adapters to 3rd-party stream processing frameworks for analyzing event streams

Documentation
-------------

- [Home](http://rbmhtechnology.github.io/eventuate/)
- [Overview](http://rbmhtechnology.github.io/eventuate/overview.html)
- [Architecture](http://rbmhtechnology.github.io/eventuate/architecture.html)
- [User guide](http://rbmhtechnology.github.io/eventuate/user-guide.html)
- [Reference](http://rbmhtechnology.github.io/eventuate/reference.html)
- [API docs](http://rbmhtechnology.github.io/eventuate/latest/api/index.html)
- [Articles](http://rbmhtechnology.github.io/eventuate/resources.html)
- [FAQs](http://rbmhtechnology.github.io/eventuate/faq.html)

Project
-------

- [Downloads](http://rbmhtechnology.github.io/eventuate/download.html)
- [Contributing](http://rbmhtechnology.github.io/eventuate/developers.html)

Community
---------

- [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/RBMHTechnology/eventuate?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
- [Mailing list](https://groups.google.com/forum/#!forum/eventuate)

Shouldn't CRDTs be more educated?
---------

- In a happy world where nothing bad ever happens, there is nothing to worry about (*). We can even live without CRDTs or conflict resolution algorithms.
(*)This is not totally true because even without bad things happening like network partitions or nodes going down, there is still a chance of concurrent operations modifying the same data, hence generating conflicts. But the scenario help me to explain the point).
![no-conflict](./img/no-conflict.gif)
If [Ariel Arnaldo Ortega](https://en.wikipedia.org/wiki/Ariel_Ortega) would like to play a Match he would only have to inform to his friends and the first of them who responds will play. The other one will watch it on TV and Ortega and his opponent will face in the field.

- But guess what?, s**t happens and more often than we could guess.
![conflict](./img/conflict.gif)
So, after Ortega had been inform his friends, a network partition could had occured. In this situation, if [Marcelo Gallardo](https://en.wikipedia.org/wiki/Marcelo_Gallardo) would accept the challenge, [Ramiro Funes Mori](https://en.wikipedia.org/wiki/Ramiro_Funes_Mori) would not notice and he could accept it too. If the system were not builded for handling this conflict we couldn't guess the final state of the Match and the three of them could end up in the field, with one of them having to back gome early. Such a mess!

- So, Should we resignate ourselfes to live with conflicts? Of course not, conflict resolution algorithms and CRDTs are here to save us.
![crdt](./img/crdt.gif)
If the system were builded with CRDTs for conflict resolution we could have avoided all the mess. Even if both players were been accepted to play by the system, the CRDT would only accept one of them. Let's say that the player who is currently in activity will have priority. Damn youngs, show some respect to the elders!
But wait! Gallardo will still show up in the field to play against Ortega. Did anyone tell him? Well, the CRDT had been saved the conflict but he also had been rude with Gallardo not telling him he has been kicked out.

- Well, we should be thankfoul to he CRDT because at the end he had saved us for any conflict. But shouldn't the CRDT be more educated?
![cerdt](./img/cerdt.gif)
So please CRDT, if Gallardo has been kicked out at least apologize to him.
