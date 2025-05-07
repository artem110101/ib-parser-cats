# ib-parser-cats

A small command-line utility that connects to the Interactive Brokers (IB) API, downloads historical bars for a fixed set of indices/options—capturing both **prices and option-implied volatility**—and bulk-inserts the results into PostgreSQL.

## Why Scala 3 + Cats Effect
* Type-safe, functional style with minimal boilerplate.
* Cats Effect & FS2 deliver pure, non-blocking I/O and composable streaming.
* Skunk supplies compile-time validated SQL and safe Postgres access.

## Core data structures & concurrency
| Role                                   | Primitive / Type                                        |
|----------------------------------------|---------------------------------------------------------|
| Monotonic request ID generator         | `AtomicInteger`                                         |
| Fast, thread-safe maps                 | `TrieMap`                                               |
| Per-request completion signalling      | `Promise[Unit]`                                         |
| Continuous data ingestion & streaming  | `fs2.Stream`                                            |

These lock-free structures and primitives enable the dedicated IB reader thread to ingest data concurrently while the main program aggregates and persists it efficiently.

## Run
