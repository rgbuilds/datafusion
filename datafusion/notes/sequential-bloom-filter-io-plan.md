<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Plan: Remove Sequential Parquet Bloom-Filter I/O

## Objective

Reduce remote object-store latency during Parquet row-group Bloom-filter
pruning by replacing the current serial row-group/column read loop with bounded
or batched I/O.

The optimization must preserve:

- conservative pruning semantics;
- support for missing and partially available Bloom filters;
- support for files that omit `bloom_filter_length`;
- custom `ParquetFileReaderFactory` behavior;
- bytes-scanned and pruning metrics;
- bounded memory and request concurrency.

## Target workload

This optimization is intended for a specific workload:

- Parquet files contain Split Block Bloom Filters;
- the query has equality or `IN` predicates that produce literal guarantees;
- min/max statistics leave many row groups as candidates;
- the Bloom filters can eliminate many of those candidates;
- storage has meaningful per-request latency.

A representative query is a point lookup on an unsorted, high-cardinality key:

```sql
SELECT ...
FROM remote_parquet
WHERE user_id = 'target';
```

This is not a universal Parquet optimization. It should be evaluated separately
from scans where Bloom filters are absent, min/max statistics already prune
most groups, or data decoding dominates execution time.

## Current behavior

`FiltersPreparedParquetOpen::load_bloom_filters`:

1. discovers columns referenced by literal guarantees;
2. iterates over every surviving row group;
3. iterates over each relevant Parquet column;
4. calls and awaits
   `ParquetRecordBatchStreamBuilder::get_row_group_column_bloom_filter`;
5. inserts each successfully loaded `Sbbf` into a per-row-group map.

Each pair is completed before the next read begins.

In parquet-rs, one request normally reads the encoded Bloom header and bitset.
When `bloom_filter_length` is absent, reading a filter requires two serial
requests:

1. read an estimated header range;
2. parse `num_bytes` and read the bitset.

For `R` surviving row groups and `C` literal columns, DataFusion therefore
performs up to `R * C` serialized filter operations and potentially
`2 * R * C` serialized range requests.

Relevant implementation locations:

- `datafusion/datasource-parquet/src/opener/mod.rs`
  - `FiltersPreparedParquetOpen::load_bloom_filters`
- `datafusion/datasource-parquet/src/reader.rs`
  - `AsyncFileReader::get_bytes`
  - `AsyncFileReader::get_byte_ranges`
- parquet-rs:
  - `ParquetRecordBatchStreamBuilder::get_row_group_column_bloom_filter`
  - Bloom-header parsing and `Sbbf` construction

## Performance model

Ignoring transfer time, the current latency is approximately:

```text
serial_time = filter_requests * request_latency
```

With bounded parallelism `P`:

```text
parallel_time ~= ceil(filter_requests / P) * request_latency
```

Example:

- 128 surviving row groups;
- 2 predicate columns;
- one request per filter;
- 10 ms request latency.

The serialized latency floor is approximately 2.56 seconds. Parallelism of 16
has an ideal latency floor of approximately 160 ms, before service, parsing,
and transfer overhead.

The benchmark must report actual end-to-end results rather than presenting
this idealized ratio as the expected production speedup.

## Design constraints

### A single `AsyncFileReader` is mutably borrowed

The existing parquet-rs method takes `&mut self`, so DataFusion cannot place
multiple calls on one builder into `FuturesUnordered`.

### Reader factories may be custom

Creating one reader per Bloom filter would technically enable concurrency, but
it could:

- repeatedly register metrics;
- create many connections or file handles;
- bypass assumptions in custom reader factories;
- introduce unbounded resource use.

That approach should not be the default design.

### Filter lengths may be missing

Footer metadata may contain an offset without a length. A batch implementation
must retain the current two-stage header/bitset behavior.

### Error isolation matters

Current behavior logs an individual filter error, increments
`predicate_evaluation_errors`, and continues conservatively. A single failed
batched request must not cause incorrect pruning or fail the entire query.

## Recommended architecture

Prefer a parquet-rs batch API operating on one `AsyncFileReader`, followed by a
small DataFusion integration.

### parquet-rs API

Add an asynchronous method conceptually equivalent to:

```rust
get_row_group_column_bloom_filters(requests)
```

where each request identifies a row group and column. The exact API should
follow parquet-rs conventions and preserve request/result ordering.

The implementation should:

1. inspect footer metadata for all requests;
2. return `None` entries without I/O when no Bloom offset exists;
3. partition present filters into known-length and unknown-length sets;
4. fetch known-length encoded filters with `get_byte_ranges`;
5. fetch unknown-length header estimates with `get_byte_ranges`;
6. parse headers and calculate bitset ranges;
7. fetch unknown-length bitsets with a second `get_byte_ranges`;
8. validate algorithm, compression, hash, and lengths as the existing scalar
   method does;
9. construct `Sbbf` values;
10. return per-request outcomes so DataFusion can continue conservatively.

Implement the existing scalar method in terms of the batch primitive, or share
one parsing helper, to prevent behavior drift.

### Bounded batches

Do not submit every filter in a very large file as one unbounded operation.
Process requests in configurable or internally fixed chunks.

Initial benchmark values:

- batch sizes: 8, 16, 32, 64;
- row groups: 16, 128, 1024;
- predicate columns: 1, 2, 8.

The initial implementation can use a conservative internal batch size without
adding a DataFusion configuration option. Add a user-facing option only if
measurements show stores need materially different tuning.

### DataFusion integration

In `load_bloom_filters`:

1. build request descriptors only for surviving row groups and literal
   columns;
2. omit requests whose footer metadata has no Bloom offset;
3. invoke the batch API in bounded chunks;
4. place successful results into the existing
   `Vec<BloomFilterStatistics>`;
5. preserve the current per-row-group column names and physical type metadata;
6. count errors and leave failed filters absent;
7. run the existing conservative pruning path.

Retain the dedicated replacement reader used for subsequent decoding unless a
separate benchmark proves that reader reuse is safe and valuable.

## DataFusion-only fallback

If an upstream parquet-rs batch API is not initially feasible, prototype a
bounded worker-pool design:

- create at most `P` readers through `ParquetFileReaderFactory`;
- assign multiple filter requests sequentially to each reader;
- execute the `P` worker queues concurrently;
- keep `P` small and fixed for the experiment.

Do not create one reader per filter.

Use this fallback primarily to establish the performance ceiling and collect
benchmark evidence. The upstream batch API remains preferable because it:

- uses `AsyncFileReader::get_byte_ranges`;
- requires fewer reader instances;
- centralizes Bloom encoding details in parquet-rs;
- can benefit other parquet-rs consumers.

## Preliminary cleanup

Land or include these low-risk changes before interpreting benchmarks:

1. Skip Bloom setup when
   `remaining_row_group_count() == 0`, rather than checking whether the access
   plan vector has zero slots.
2. Avoid allocating a dense per-file Bloom vector before determining that no
   Bloom work is required, where practical.
3. Remove the nested `bloom_filter_eval_time` timer so reported evaluation
   time is not double counted.
4. Keep Bloom I/O time and Bloom predicate-evaluation time distinguishable.
   Add a dedicated load timer only if it is useful beyond the benchmark.

These changes do not solve serialized I/O, but they make measurements easier
to interpret.

## Correctness tests

Test the batch primitive and DataFusion integration for:

- no Bloom offset;
- one known-length filter;
- several known-length filters;
- unknown `bloom_filter_length`;
- mixed known and unknown lengths;
- multiple row groups and multiple columns;
- unsupported or malformed header metadata;
- truncated header;
- truncated bitset;
- invalid offset or length conversion;
- one failed filter among successful filters;
- duplicate requests, if the API allows them;
- empty request list;
- request count larger than the batch size;
- row groups already skipped by statistics or file-range pruning;
- unsupported DataFusion scalar types;
- Decimal128 physical encodings;
- equality, OR, and `IN` predicates;
- no false negatives compared with the existing scalar path.

For malformed or failed reads, verify that DataFusion retains the affected row
group unless another valid statistic independently proves it can be skipped.

## Object-store benchmark

Add an instrumented `ObjectStore` wrapper that records:

- `get_range` calls;
- `get_ranges` calls;
- number of ranges;
- bytes requested;
- maximum concurrent requests;
- injected latency;
- wall-clock duration.

Benchmark at least:

- latency: 0, 1, 5, 10, 25 ms;
- row groups: 16, 128, 1024;
- literal columns: 1, 2, 8;
- Bloom length present and absent;
- Bloom hit rate: 0%, 10%, 100% of groups retained;
- batch sizes: 1, 8, 16, 32, 64.

Report:

- Bloom-load wall time;
- time to first output batch;
- total query wall time;
- range-request count;
- bytes scanned;
- peak concurrent requests;
- peak memory attributable to loaded filters.

Include three storage modes:

1. in-memory store with injected latency, for deterministic scaling;
2. local filesystem, to detect overhead regressions;
3. an optional S3-compatible benchmark, for external validation.

The benchmark should compare:

- current scalar serial reads;
- DataFusion worker-pool prototype, if built;
- parquet-rs batched ranges.

## Expected outcomes

For latency-dominated Bloom loading with enough requests, bounded/batched I/O
should materially reduce the Bloom phase. A realistic target is:

- at least 4x faster Bloom loading at 10 ms injected latency with 128 row
  groups, 2 columns, and a batch/concurrency width of at least 8;
- no material regression at zero injected latency;
- unchanged bytes read for known-length filters, excluding intentional range
  coalescing;
- bounded memory and request concurrency.

End-to-end gains will be smaller and workload-dependent. They should be
reported only for the target Bloom-effective workload and not generalized to
all selective Parquet queries.

## Risks and mitigations

### Object-store throttling

Risk: excessive concurrency increases tail latency or triggers throttling.

Mitigation: bounded batches, conservative defaults, and concurrency metrics.

### Read amplification

Risk: coalescing distant ranges transfers substantially more bytes.

Mitigation: batch exact ranges through `get_byte_ranges` first. Add contiguous
range coalescing only with a maximum-gap policy and separate byte metrics.

### Batch failure scope

Risk: one `get_ranges` failure loses all filters in a batch.

Mitigation: conservatively retain affected groups. Consider retrying failed
batches as smaller batches or scalar requests only if benchmarks justify the
extra complexity.

### Memory pressure

Risk: parallel reads materialize many encoded filters simultaneously.

Mitigation: bounded batch size and prompt insertion/drop of temporary buffers.
Measure peak memory for large Bloom filters.

### Metrics inflation

Risk: multiple readers or retries register duplicate counters or count bytes
twice.

Mitigation: prefer one-reader batch I/O, document retry accounting, and assert
request/byte metrics in tests.

### Upstream dependency

Risk: the clean design requires a parquet-rs API addition and release.

Mitigation: develop the parquet-rs API first or maintain a short-lived
DataFusion prototype solely for benchmark evidence. Avoid duplicating private
Thrift Bloom-header parsing in DataFusion.

## Acceptance criteria

- Results are identical to the scalar implementation across supported types
  and malformed/missing-filter cases.
- No row group is pruned because a Bloom read failed.
- Request concurrency and temporary memory are bounded.
- The target latency benchmark shows at least a 4x Bloom-load improvement.
- Zero-latency/local benchmarks show no material regression.
- Bytes and pruning metrics remain correct and the Bloom evaluation timer is
  not double counted.
- `cargo fmt --all`, workspace clippy, and required extended tests pass in
  DataFusion.
- parquet-rs unit, integration, formatting, and clippy checks pass for the
  upstream API change.

## PR strategy

Prefer a staged contribution:

1. DataFusion benchmark plus low-risk guard/timer cleanup.
2. parquet-rs batched Bloom-read API with dedicated tests and benchmarks.
3. DataFusion adoption of the batch API and end-to-end benchmark results.

This split gives reviewers evidence before changing I/O behavior and keeps the
cross-repository API work independently reviewable.
