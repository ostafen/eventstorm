# EventStormDB

![build & test workflow](https://github.com/ostafen/eventstorm/actions/workflows/build-test.yml/badge.svg)

**EventStormDB** implements a state-transition database on top of PostgreSQL. It acts a a conversion layer between any [EventStoreDB](https://www.eventstore.com) compatible client and the SQL language.

## Why EventStormDB?

Event Store is a purpose-built database, offering advanced stream analysis and querying features that are otherwise difficult to replicate with SQL or a data analysis pipeline. Its [Projection Subsystem](https://developers.eventstore.com/server/v5/projections.html#introduction-to-projections) provides the ability to pipe existing streams through transformation functions that write other streams that which can be then consumed directly or trigger further transformations.

However, due to its use-case specific nature, it is not likely for a real world application to completely rely on Event Store. 

On the countrary, Postgres is a general purpose SQL database, which is developer friendly and battle-tested.
It ships with a miryad of community developed extensions and it is well-supported by major cloud providers like Amazon Web Services and Google Cloud Platform.

That is why implementing a simple eventsourcing layer on the top of PostgreSQL is a common and cheaper choice with respect to the cost of switching and maintain an EventStoreDB cluster.

**EventStorm** was born to provide the full power of EventStore stream processing primitives to PostgreSQL users without additional dependencies.
It is meant to be a replacement for EventStore expecially in following scenarios:

- You are aready running PostgreSQL, but want to exploit the power of EventStore subscriptions and/or projections;
- You want to estimate the impact of migrating to EventStoreDB.

You should rather use **EventStoreDB** if:

- you need a distributed database;
- you have to deal with tremendous write workloads which cannot be handled by PostgreSQL (EventStoreDB makes use of append-only log).

 