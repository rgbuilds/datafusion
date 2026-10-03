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

# Plan: Conditional and Lazy Fully-Matched Row-Group Detection

## Objective

Avoid constructing and evaluating an inverted pruning predicate when no later
scan stage can use the resulting `fully_matched` row-group flags.

The optimization must preserve:

- row-group pruning decisions;
- `LIMIT` pruning;
- page-index pruning and page-index load avoidance;
- suppression of row-level pushed-down filters for fully matched groups;
- existing conservative null handling;
- pruning and error metrics.

## Current behavior

`RowGroupAccessPlanFilter::prune_by_statistics_inner` performs two conceptually
different operations:

1. It evaluates the normal pruning predicate and skips row groups that cannot
   match.
2. For every surviving candidate, it calls
   `identify_fully_matched_row_groups`.

The second operation:

- wraps the original expression in `NOT`;
- adds `IS NULL` disjuncts for nullable referenced columns;
- simplifies the expression;
- builds another `PruningPredicate`;
- extracts row-group statistics again;
- evaluates the inverted predicate;
- marks groups pruned by the inverted predicate as fully matched.

This is performed unconditionally whenever statistics pruning succeeds and at
least one row group survives.

The resulting flags are consumed only by:

- `RowGroupAccessPlanFilter::prune_by_limit`;
- `RowGroupsPrunedParquetOpen::should_load_page_index`;
- `PagePruningAccessPlanFilter`, which skips fully matched groups;
- the push decoder, which suppresses its `RowFilter` for fully matched groups.

Relevant implementation locations:

- `datafusion/datasource-parquet/src/row_group_filter.rs`
  - `prune_by_statistics_inner`
  - `identify_fully_matched_row_groups`
- `datafusion/datasource-parquet/src/opener/mod.rs`
  - `FiltersPreparedParquetOpen::prune_row_groups`
  - `RowGroupsPrunedParquetOpen::should_load_page_index`
  - `RowGroupsPrunedParquetOpen::build_stream`
- `datafusion/datasource-parquet/src/page_filter.rs`
- `datafusion/datasource-parquet/src/push_decoder.rs`

## Why this should be lazy

Fully-matched detection has no effect when all of the following are true:

- there is no applicable order-insensitive `LIMIT`;
- page pruning cannot run for a surviving row group;
- row-level filter pushdown is disabled or no row filter can be built.

In that case, only the normal may-match result affects the scan. The inverted
predicate is pure overhead.

The common default configuration makes this case plausible:

- row-group statistics pruning is enabled;
- row-filter pushdown is disabled by default;
- page-index support is enabled, but many files do not contain column and
  offset indexes;
- many queries do not have a usable `LIMIT`.

## Proposed design

Separate ordinary pruning from fully-matched classification.

### Phase 1: Perform normal row-group pruning

Change the statistics-pruning path so it:

1. evaluates the normal predicate;
2. updates skipped/matched statistics metrics;
3. records or returns surviving candidate indexes;
4. does not immediately evaluate the inverted predicate.

Keep the current vectorized evaluation over all eligible row groups.

Avoid adding a public boolean argument such as `detect_fully_matched` to the
existing API if possible. Prefer one of:

- a separate crate-private method that classifies surviving groups; or
- an internal options/policy type with named semantics.

The distinction between "may match" and "fully matches" should remain explicit
in the API.

### Phase 2: Decide whether classification has a consumer

After normal pruning, derive a `needs_fully_matched_detection` decision from
the prepared scan and surviving access plan.

Detection is needed when at least one of these conditions holds:

1. `prepared.limit.is_some() && !prepared.preserve_order`;
2. a row-level pushed-down `RowFilter` will be installed;
3. page-index pruning is possible for at least one surviving row group and one
   page-predicate column.

For page pruning, use footer offsets to establish applicability without loading
the page index:

- map page-predicate columns to Parquet leaf columns;
- inspect surviving row groups;
- require both `column_index_offset` and `offset_index_offset`.

Refactor the existing footer-offset logic in `should_load_page_index` into a
helper so the applicability check has one implementation.

For the first implementation, `prepared.pushdown_filters && predicate.is_some()`
may conservatively enable detection. A follow-up can move row-filter candidate
prebuilding earlier and enable detection only when a non-empty
`RowFilterContext` exists.

### Phase 3: Classify only when needed

When the decision is true, invoke the existing inverted-predicate logic for
the surviving candidates.

Preserve these correctness properties:

- include `IS NULL` for nullable predicate columns;
- use `missing_null_counts_as_zero: false`;
- treat simplification, predicate construction, or evaluation errors as
  "unable to prove fully matched";
- never change an ordinary row-group prune result because classification
  failed.

When the decision is false, leave every surviving group's `fully_matched` flag
false.

### Phase 4: Reuse statistics when classification is active

Treat this as a follow-up unless it keeps the first change simple.

Both normal and inverted predicates may request the same min, max, null-count,
and row-count arrays. Introduce a per-file evaluation cache keyed by:

- logical column identity;
- statistics type;
- missing-null-count policy.

The missing-null-count policy must be part of the key because ordinary pruning
uses `true`, while fully-matched proof uses `false`.

Do not cache untrusted byte-array values without retaining the existing
per-row-group masking behavior.

## Implementation sequence

1. Extract a helper that reports whether page-index pruning is possible from
   footer offsets for the current predicate columns and access plan.
2. Separate fully-matched classification from normal statistics pruning.
3. Add the consumer decision in the opener.
4. Preserve the current behavior when any consumer exists.
5. Add observability suitable for benchmarks. Prefer a benchmark-local counter
   or an existing timer split over a permanent user-facing metric unless the
   metric has independent operational value.
6. Add the optional statistics cache in a separate commit or PR after the lazy
   behavior is measured.

## Correctness tests

Add focused tests for the decision matrix:

- no limit, no page index, pushdown disabled:
  - ordinary pruning is unchanged;
  - no group is classified as fully matched.
- usable `LIMIT`:
  - fully matched groups are still found;
  - `prune_by_limit` keeps enough fully matched groups;
  - order-sensitive limits do not trigger classification solely for `LIMIT`.
- page index present for a predicate column:
  - classification still runs;
  - fully matched groups avoid page-index work.
- page index absent:
  - classification is skipped when there is no other consumer;
  - page-index load remains skipped.
- page index present only on already-pruned groups:
  - classification is skipped when there is no other consumer.
- row-filter pushdown enabled:
  - fully matched groups still suppress the `RowFilter`;
  - partially matched groups retain the filter.
- nullable columns and missing null counts:
  - no group is incorrectly marked fully matched.
- predicate inversion/build/evaluation failure:
  - groups remain scannable and unclassified.
- initial `ParquetAccessPlan` containing `Selection` and `Skip` entries:
  - only surviving groups participate.

Extend existing tests in:

- `row_group_filter.rs`;
- opener page-index tests;
- push-decoder fully-matched tests;
- `datafusion/core/tests/parquet/row_group_pruning.rs`.

## Performance benchmark

Add a Criterion benchmark to `datafusion-datasource-parquet` that constructs
metadata with configurable:

- row-group count: 1, 16, 128, 1024;
- predicate width: 1, 4, 8 columns;
- fraction surviving normal pruning;
- nullable versus non-nullable columns;
- presence or absence of consumers.

Measure these cases:

1. normal pruning only, classification skipped;
2. current-equivalent eager classification;
3. classification required by `LIMIT`;
4. classification required by page pruning;
5. classification required by row-filter pushdown.

Primary measurements:

- wall-clock time per file;
- `statistics_eval_time`;
- allocations if the benchmark environment supports allocation tracking.

Expected result for the no-consumer case:

- eliminate one predicate build and one statistics evaluation;
- reduce statistics-pruning CPU by approximately 30–50% when many groups
  survive;
- no regression when every group is removed by the normal pass.

End-to-end validation should include many small Parquet files because metadata
and pruning CPU are most visible there.

## Risks and mitigations

### Missing a consumer

Risk: classification is skipped even though a later stage relies on the flags,
reducing performance or changing `LIMIT` planning.

Mitigation: centralize the decision, enumerate all consumers in its
documentation, and add one test per consumer.

### Circular page-index decision

Risk: page-index loading currently uses fully-matched flags to decide whether
loading is useful, while lazy classification wants to know whether a page
index exists before producing those flags.

Mitigation: split "footer says a relevant index exists" from "all surviving
groups are fully matched." The first check determines whether classification
has a page-pruning consumer; the second remains an optimization after
classification.

### Row-filter applicability

Risk: `pushdown_filters` is true but no pushdown candidate can be constructed,
causing unnecessary classification.

Mitigation: begin conservatively, then optionally move candidate prebuilding
earlier so the decision uses an actual `RowFilterContext`.

### Metric changes

Risk: `row_groups_pruned_statistics.fully_matched` decreases when
classification is intentionally skipped.

Mitigation: document that `fully_matched` counts proven groups used by enabled
optimizations, not all theoretically provable groups. Verify that total
pruned/matched accounting remains internally consistent.

## Acceptance criteria

- Query results and ordinary row-group pruning decisions are unchanged.
- All three consumers retain existing behavior when enabled and applicable.
- No inverted predicate is built in the no-consumer path.
- Existing fully-matched, page-index, limit, and pushdown tests pass.
- A benchmark demonstrates a repeatable pruning-CPU improvement in the
  no-consumer case without a material regression in consumer cases.
- `cargo fmt --all`, workspace clippy, and the required extended tests pass.

## PR strategy

Prefer two reviewable changes:

1. Lazy/conditional fully-matched detection with tests and benchmarks.
2. Optional reuse of extracted statistics between normal and inverted
   evaluation.

The first PR should not change public configuration or query semantics.
