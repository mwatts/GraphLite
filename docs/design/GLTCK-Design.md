# GLTCK: GraphLite Technology Compatibility Kit -- Design Document

**Status**: Draft
**Date**: 2025-02-18
**Branch**: feature/compat-test

## 1. Objective

Establish a comprehensive conformance and regression testing framework for GraphLite, drawing from ISO GQL standard features, openCypher TCK scenarios, and standard graph datasets. The framework must track every test case with a clear outcome: **pass**, **fail**, or **unsupported** -- distinguishing features GraphLite has not yet implemented from features that are broken.

---

## 2. Test Outcome Model

Every test in the GLTCK produces one of three outcomes:

| Outcome | Meaning | Action |
|---------|---------|--------|
| **pass** | Test executes and produces the expected result | None |
| **fail** | Test executes but produces an incorrect result, or panics | Bug -- must be investigated |
| **unsupported** | GraphLite does not yet implement the feature under test | Tracked for future implementation |

An **unsupported** outcome is NOT a failure. It means the test was recognized, executed, and GraphLite correctly reported that the feature is not available (e.g., returned a parse error or "not implemented" error for a valid GQL construct). This distinction prevents CI from breaking on known gaps while still tracking conformance progress over time.

### Implementation

Tests marked unsupported are annotated with `#[ignore]` and a comment referencing the GLTCK ID. A manifest file (`tests/tck/manifest.toml`) tracks the status of every test ID:

```toml
[GLTCK-001]
status = "pass"        # pass | fail | unsupported
feature = "MATCH basic node pattern"
source = "openCypher TCK"

[GLTCK-042]
status = "unsupported"
feature = "OPTIONAL MATCH"
source = "openCypher TCK"
reason = "OPTIONAL MATCH not yet implemented"
```

A CI script validates that:
1. Every `#[ignore]` test has a corresponding `unsupported` entry in the manifest
2. No `pass` entry in the manifest corresponds to an `#[ignore]` test (stale ignore)
3. No `fail` entries exist (all failures must be triaged to either `pass` after fix or `unsupported`)

---

## 3. Test Sources

### GLTCK-100 Series: openCypher TCK Adaptation

The openCypher TCK (v2024.2, Apache-2.0) is the highest-value source. It contains 2000+ Gherkin test scenarios organized into 35+ feature categories that are actively aligning with ISO GQL.

| ID | Option | Description | Effort | Priority |
|----|--------|-------------|--------|----------|
| **GLTCK-100** | Clone openCypher TCK feature files | Copy `.feature` files from `opencypher/openCypher/tck/` into `tests/tck/features/` | Low | P0 |
| **GLTCK-101** | Cypher-to-GQL translation layer | Build a query rewriter that maps Cypher syntax to GraphLite GQL syntax (`CREATE` to `INSERT`, property access, etc.) | Medium | P0 |
| **GLTCK-102** | Gherkin parser for Rust | Use `cucumber-rs` crate or build a custom `.feature` file parser that extracts Given/When/Then steps | Medium | P0 |
| **GLTCK-103** | TCK step definitions | Implement Rust step definitions: graph setup, query execution, result assertion, error assertion | Medium | P0 |
| **GLTCK-104** | TCK manifest generation | Auto-generate `manifest.toml` entries from parsed `.feature` files with initial `unsupported` status | Low | P1 |
| **GLTCK-105** | Selective TCK execution | Support `TCK_INCLUDE`/`TCK_EXCLUDE` env vars to run subsets (e.g., `TCK_INCLUDE=clauses/match-where`) | Low | P1 |
| **GLTCK-106** | TCK result reporting | Generate a conformance report showing pass/fail/unsupported counts per category | Low | P2 |

**TCK clause categories to adapt (17):**

| ID | Category | Scenarios | Notes |
|----|----------|-----------|-------|
| GLTCK-110 | `match` | Core MATCH patterns | High priority -- GraphLite core |
| GLTCK-111 | `match-where` | MATCH with WHERE filtering | High priority |
| GLTCK-112 | `return` | RETURN projection | High priority |
| GLTCK-113 | `return-orderby` | ORDER BY | High priority |
| GLTCK-114 | `return-skip-limit` | SKIP/LIMIT pagination | High priority |
| GLTCK-115 | `create` | CREATE (maps to INSERT) | Requires GLTCK-101 translation |
| GLTCK-116 | `set` | SET property updates | High priority |
| GLTCK-117 | `remove` | REMOVE properties | High priority |
| GLTCK-118 | `delete` | DELETE nodes/edges | High priority |
| GLTCK-119 | `with` | WITH clause | High priority |
| GLTCK-120 | `with-where` | WITH + WHERE | Medium priority |
| GLTCK-121 | `with-orderby` | WITH + ORDER BY | Medium priority |
| GLTCK-122 | `with-skip-limit` | WITH + SKIP/LIMIT | Medium priority |
| GLTCK-123 | `union` | UNION operations | Medium priority |
| GLTCK-124 | `unwind` | UNWIND/FOR | May need GQL adaptation |
| GLTCK-125 | `merge` | MERGE (upsert) | Likely unsupported initially |
| GLTCK-126 | `call` | Procedure calls | Likely unsupported initially |

**TCK expression categories to adapt (18):**

| ID | Category | Notes |
|----|----------|-------|
| GLTCK-130 | `aggregation` | COUNT, SUM, AVG, MIN, MAX |
| GLTCK-131 | `boolean` | AND, OR, NOT, XOR |
| GLTCK-132 | `comparison` | =, <>, <, >, <=, >= |
| GLTCK-133 | `conditional` | CASE expressions |
| GLTCK-134 | `existentialSubqueries` | EXISTS subqueries |
| GLTCK-135 | `graph` | Graph expressions |
| GLTCK-136 | `list` | List operations |
| GLTCK-137 | `literals` | Literal values |
| GLTCK-138 | `map` | Map/dictionary operations |
| GLTCK-139 | `mathematical` | Arithmetic operations |
| GLTCK-140 | `null` | NULL handling |
| GLTCK-141 | `path` | Path expressions |
| GLTCK-142 | `pattern` | Pattern expressions |
| GLTCK-143 | `precedence` | Operator precedence |
| GLTCK-144 | `quantifier` | Quantified expressions |
| GLTCK-145 | `string` | String operations |
| GLTCK-146 | `temporal` | Date/time operations |
| GLTCK-147 | `typeConversion` | Type casting |

---

### GLTCK-200 Series: ISO GQL Conformance Feature Matrix

Based on conformance documentation from Neo4j, Ultipa, and Google Spanner, build a tracking matrix against ISO/IEC 39075:2024 feature IDs.

| ID | Option | Description | Effort | Priority |
|----|--------|-------------|--------|----------|
| **GLTCK-200** | GQL mandatory feature checklist | Create test cases for each mandatory feature (subclauses 4.x-22.x) | Medium | P1 |
| **GLTCK-201** | GQL optional feature matrix | Track optional features by ID (G001-G049, GA01-GA08, GB01-GB03, GC01-GC05, GD01-GD04, GE01-GE09, GF01-GF12, GG01-GG12, GH01-GH02, GL01-GL12, GP01-GP18, GQ01-GQ24, GS01-GS16, GT01-GT03, GV01-GV52) | Medium | P2 |
| **GLTCK-202** | Conformance report generator | Generate an ISO GQL conformance report from test results, grouped by feature category | Low | P2 |

**Mandatory GQL feature test areas:**

| ID | Subclause | Feature | Status |
|----|-----------|---------|--------|
| GLTCK-210 | 4.11 | Graph pattern matching | Test via GLTCK-110 |
| GLTCK-211 | 4.13 | GQL object types (NODE, RELATIONSHIP) | Test via existing tests |
| GLTCK-212 | 4.16 | Predefined value types (BOOLEAN, FLOAT, INTEGER, STRING) | Needs dedicated tests |
| GLTCK-213 | 7.1-7.3 | Session management (SESSION SET/RESET/CLOSE) | Partially implemented |
| GLTCK-214 | 8.1-8.4 | Transaction management (START/COMMIT/ROLLBACK) | Needs tests |
| GLTCK-215 | 13.2 | INSERT statement | Test via existing DML tests |
| GLTCK-216 | 13.3 | SET statement | Test via existing tests |
| GLTCK-217 | 13.4 | REMOVE statement | Needs tests |
| GLTCK-218 | 13.5 | DELETE statement | Test via existing tests |
| GLTCK-219 | 14.4 | MATCH statement | Test via GLTCK-110/111 |
| GLTCK-220 | 14.9 | ORDER BY and page statement | Test via GLTCK-113/114 |
| GLTCK-221 | 14.10-14.11 | RETURN statement | Test via GLTCK-112 |
| GLTCK-222 | 15.4 | Conditional statement | Needs tests |
| GLTCK-223 | 16.2-16.19 | Pattern/label/quantifier expressions | Needs tests |
| GLTCK-224 | 19.3-19.7 | Predicates (comparison, exists, null, type, normalized) | Needs tests |
| GLTCK-225 | 20.2-20.24 | Value expressions, CASE, aggregates, functions | Partially tested |
| GLTCK-226 | 21.1 | Names and variables | Implicitly tested |
| GLTCK-227 | 22.15 | Grouping operations | Test via GLTCK-130 |

---

### GLTCK-300 Series: Sample Dataset Integration

Standard graph datasets for regression and scale testing.

| ID | Option | Description | Size | License | Effort | Priority |
|----|--------|-------------|------|---------|--------|----------|
| **GLTCK-300** | Neo4j Movies dataset | 171 nodes, 253 edges. Translate Cypher CREATE to GQL INSERT. Embed as test fixture. | Tiny | Public | Low | P0 |
| **GLTCK-301** | Northwind dataset | ~1K nodes, ~3K edges. Business data patterns (customers, orders, products). | Small | Public domain | Low | P1 |
| **GLTCK-302** | LDBC SNB SF1 | ~3.2M nodes, ~17.3M edges. 8 node types, 23 edge types. Realistic social network. | Large | Apache-2.0 | Medium | P2 |
| **GLTCK-303** | SNAP ego-Facebook | 4,039 nodes, 88,234 edges. Stress test for traversals. | Medium | Research | Low | P3 |
| **GLTCK-304** | ICIJ Panama Papers | ~810K entities. Real-world messy data for robustness testing. | Large | ODbL | Medium | P3 |

**Movies dataset test queries (GLTCK-300 sub-tests):**

| ID | Query Pattern | Tests |
|----|---------------|-------|
| GLTCK-310 | `MATCH (p:Person) RETURN p.name` | Basic node match with property |
| GLTCK-311 | `MATCH (m:Movie) WHERE m.released > 2000 RETURN m.title` | Filtered match |
| GLTCK-312 | `MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p.name, m.title` | Edge traversal |
| GLTCK-313 | `MATCH (p:Person)-[:DIRECTED]->(m:Movie) RETURN p.name, count(m)` | Aggregation |
| GLTCK-314 | `MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p.name, count(m) ORDER BY count(m) DESC LIMIT 5` | Order + limit |
| GLTCK-315 | `MATCH (a:Person)-[:ACTED_IN]->(m:Movie)<-[:ACTED_IN]-(b:Person) WHERE a <> b RETURN DISTINCT a.name, b.name` | Co-actor pattern |
| GLTCK-316 | `MATCH (p:Person) RETURN count(p), avg(p.born), min(p.born), max(p.born)` | Multiple aggregates |
| GLTCK-317 | `MATCH (p:Person)-[r]->(m:Movie) RETURN type(r), count(r)` | Relationship type grouping |

---

### GLTCK-400 Series: GQL Grammar and Syntax Validation

Parser-level tests derived from GQL grammar resources.

| ID | Option | Description | Source | Effort | Priority |
|----|--------|-------------|--------|--------|----------|
| **GLTCK-400** | OpenGQL grammar samples | Parse sample `.gql` files from `opengql/grammar` repo | Apache-2.0 | Low | P1 |
| **GLTCK-401** | Packt GQL book examples | Parse and execute `.gql` files from "Getting Started with GQL" book repo | Public | Low | P1 |
| **GLTCK-402** | OlofMorra GQL parser test queries | Adapt test queries from academic GQL parser | Apache-2.0 | Low | P2 |
| **GLTCK-403** | Negative syntax tests | Queries that should fail to parse -- verify error handling | Custom | Medium | P1 |

---

### GLTCK-500 Series: Automated Bug Detection

Advanced testing techniques for finding logic bugs.

| ID | Option | Description | Effort | Priority |
|----|--------|-------------|--------|----------|
| **GLTCK-500** | Predicate partitioning (GDBMeter approach) | For any query `MATCH (n) WHERE P RETURN n`, verify that `WHERE P` UNION `WHERE NOT P` equals the full result set | Medium | P2 |
| **GLTCK-501** | Equivalent query rewriting (GRev approach) | Generate semantically equivalent query variants and check for consistent results | High | P3 |
| **GLTCK-502** | Metamorphic testing (Gamera approach) | Apply graph structural transformations that should preserve certain query results | High | P3 |

---

### GLTCK-600 Series: Performance Regression

| ID | Option | Description | Effort | Priority |
|----|--------|-------------|--------|----------|
| **GLTCK-600** | Movies dataset query benchmarks | Baseline timing for GLTCK-310 through GLTCK-317 queries | Low | P2 |
| **GLTCK-601** | LDBC SNB SF1 query benchmarks | Translate 14 Interactive read queries to GQL, benchmark on SF1 | High | P3 |
| **GLTCK-602** | Insertion throughput benchmark | Measure bulk insert performance at various scales | Medium | P2 |

---

## 4. Architecture

### Directory Structure

```
tests/
  tck/
    manifest.toml              # Test status tracking (pass/fail/unsupported per ID)
    features/                  # openCypher TCK .feature files (copied, Apache-2.0)
      clauses/
        match/
        match-where/
        return/
        ...
      expressions/
        aggregation/
        boolean/
        ...
    datasets/
      movies.gql               # Neo4j Movies translated to GQL INSERT
      northwind.gql             # Northwind translated to GQL INSERT
    gql_samples/
      opengql/                  # Sample .gql files from OpenGQL grammar
      packt/                    # Sample .gql files from Packt book
    src/
      runner.rs                 # TCK test runner
      translator.rs             # Cypher-to-GQL query translator
      manifest.rs               # Manifest parser and validator
      report.rs                 # Conformance report generator
    compat_tests.rs             # Integration test entry point
    movies_tests.rs             # Movies dataset regression tests
    gql_conformance_tests.rs    # ISO GQL mandatory feature tests
```

### Test Runner Flow

```
1. Parse .feature file -> extract scenarios
2. For each scenario:
   a. Check manifest.toml for status
   b. If status == "unsupported" -> record as unsupported, skip execution
   c. Translate Cypher Given/When steps to GQL via translator
   d. Create TestFixture with empty graph
   e. Execute setup queries (Given steps)
   f. Execute test query (When step)
   g. Compare result against Then step expectations
   h. Record outcome: pass or fail
3. Generate conformance report
```

### Cypher-to-GQL Translation (GLTCK-101)

Key mappings the translator must handle:

| Cypher | GQL (GraphLite) | Notes |
|--------|-----------------|-------|
| `CREATE (n:Label {p: v})` | `INSERT (:Label {p: v})` | Node creation |
| `CREATE (a)-[:TYPE]->(b)` | `INSERT (a)-[:TYPE]->(b)` | Edge creation |
| `MERGE` | *unsupported* | Mark scenarios as unsupported |
| `UNWIND list AS x` | `FOR x IN list` | If implemented |
| `type(r)` | `type(r)` | Same in GQL |
| `id(n)` | *unsupported* | Internal ID access |
| `labels(n)` | *unsupported* | Label introspection |
| `keys(n)` | *unsupported* | Property key introspection |
| `n.prop` | `n.prop` | Same |
| `RETURN *` | `RETURN *` | Same |
| `OPTIONAL MATCH` | `OPTIONAL MATCH` | If implemented |

The translator returns a `TranslationResult`:
```rust
enum TranslationResult {
    /// Successfully translated to GQL
    Translated(String),
    /// Feature not supported in GraphLite -- mark test as unsupported
    Unsupported { reason: String },
    /// Translation error -- mark test as fail
    Error { message: String },
}
```

When the translator returns `Unsupported`, the test runner automatically records the test as `unsupported` in the manifest without executing it. This is the key mechanism for distinguishing "not yet implemented" from "broken".

### Manifest File (manifest.toml)

The manifest is the single source of truth for test status. It is checked into version control and updated as features are implemented.

```toml
# Auto-generated header -- do not edit above this line
# Generated from: openCypher TCK 2024.2
# Total scenarios: 2000+
# Pass: 847  |  Fail: 0  |  Unsupported: 1153

[GLTCK-110.001]
status = "pass"
feature = "Match all nodes"
category = "clauses/match"
source_file = "Match1.feature"
scenario = "Match all nodes"

[GLTCK-111.001]
status = "pass"
feature = "Filter node with property predicate"
category = "clauses/match-where"
source_file = "MatchWhere1.feature"
scenario = "Filter node with property predicate on a single variable"

[GLTCK-125.001]
status = "unsupported"
feature = "MERGE node"
category = "clauses/merge"
source_file = "Merge1.feature"
scenario = "Merge node when no matching node exists"
reason = "MERGE not implemented"

[GLTCK-134.001]
status = "unsupported"
feature = "EXISTS subquery"
category = "expressions/existentialSubqueries"
source_file = "ExistentialSubquery1.feature"
scenario = "Simple exists subquery"
reason = "EXISTS subquery not implemented"
```

### CI Integration

```yaml
# In .github/workflows/ci.yml
- name: Run GLTCK conformance tests
  run: cargo test --test compat_tests -- --include-ignored 2>&1 | tee tck-results.txt

- name: Validate manifest consistency
  run: cargo run --bin gltck-validate

- name: Check for regressions
  run: |
    # Fail CI if any test marked "pass" in manifest now fails
    # Fail CI if any test marked "unsupported" now passes (promote it!)
    cargo run --bin gltck-check-regressions
```

---

## 5. Recommendations

### Phase 1: Foundation (P0)

| ID | Recommendation | Rationale |
|----|----------------|-----------|
| **GLTCK-100** | Clone openCypher TCK `.feature` files | Apache-2.0 compatible, 2000+ scenarios, actively maintained |
| **GLTCK-101** | Build Cypher-to-GQL translator | Required for TCK adaptation; centralizes syntax mapping |
| **GLTCK-102** | Integrate `cucumber-rs` or build custom Gherkin parser | Needed to consume `.feature` files in Rust tests |
| **GLTCK-103** | Implement core step definitions | Connect Gherkin steps to GraphLite `TestFixture` API |
| **GLTCK-300** | Translate and embed Movies dataset | Immediate smoke test value, 171 nodes is tiny |

### Phase 2: Coverage (P1)

| ID | Recommendation | Rationale |
|----|----------------|-----------|
| **GLTCK-104** | Auto-generate manifest from `.feature` files | Ensures every TCK scenario is tracked |
| **GLTCK-105** | Selective TCK execution via env vars | Essential for development workflow |
| **GLTCK-200** | Create ISO GQL mandatory feature tests | Independent of openCypher -- tests GQL-specific features like SESSION SET SCHEMA |
| **GLTCK-301** | Add Northwind dataset | More complex schema for richer testing |
| **GLTCK-400** | Parse OpenGQL grammar samples | Parser coverage validation |
| **GLTCK-401** | Parse Packt book GQL examples | Real-world GQL syntax validation |
| **GLTCK-403** | Add negative syntax tests | Error handling quality |

### Phase 3: Depth (P2)

| ID | Recommendation | Rationale |
|----|----------------|-----------|
| **GLTCK-106** | Conformance report generator | Visibility into conformance progress |
| **GLTCK-201** | Track GQL optional features by ID | Differentiated positioning vs other databases |
| **GLTCK-202** | Generate ISO conformance report | Marketing and documentation value |
| **GLTCK-302** | Import LDBC SNB SF1 | Scale testing with realistic data |
| **GLTCK-500** | Implement predicate partitioning | Automated logic bug detection |
| **GLTCK-600** | Performance baselines | Prevent performance regressions |

### Phase 4: Advanced (P3)

| ID | Recommendation | Rationale |
|----|----------------|-----------|
| **GLTCK-303** | SNAP ego-Facebook stress testing | Traversal performance on realistic topology |
| **GLTCK-304** | Panama Papers robustness testing | Messy real-world data edge cases |
| **GLTCK-501** | Equivalent query rewriting | Query optimizer correctness |
| **GLTCK-502** | Metamorphic testing | Systematic bug finding |
| **GLTCK-601** | LDBC SNB query benchmarks | Industry-standard performance comparison |

---

## 6. Key Design Decisions

### GLTCK-700: Custom Gherkin parser vs cucumber-rs

| ID | Option | Pros | Cons |
|----|--------|------|------|
| GLTCK-700 | Use `cucumber-rs` crate | Standard Cucumber compliance; async support; built-in reporting | Requires Rust 1.88+; heavier dependency; less control over translation layer |
| GLTCK-701 | Custom `.feature` parser | Lightweight; full control; simpler Cypher-to-GQL translation integration; no MSRV issues | Must implement Gherkin parsing; no ecosystem tooling |

**Recommendation**: GLTCK-701 (custom parser). The translation layer between Cypher and GQL is non-trivial and benefits from tight integration with parsing. A custom parser also avoids MSRV constraints and keeps the dependency footprint minimal. The Gherkin format is simple enough (Given/When/Then with docstrings and tables) that a purpose-built parser is straightforward.

### GLTCK-710: Test execution model

| ID | Option | Description |
|----|--------|-------------|
| GLTCK-710 | Each scenario gets its own TestFixture | Maximum isolation; slower |
| GLTCK-711 | Scenarios share a TestFixture per feature file | Faster; requires careful Given step handling |
| GLTCK-712 | Scenarios share a TestFixture per category | Fastest; risk of test interference |

**Recommendation**: GLTCK-710 (per-scenario isolation). The `TestFixture` already uses unique schema/graph names. Per-scenario isolation matches the TCK's self-contained design and prevents interference. The performance cost is acceptable because each scenario only creates a small graph.

### GLTCK-720: Manifest-driven vs code-driven unsupported tracking

| ID | Option | Description |
|----|--------|-------------|
| GLTCK-720 | TOML manifest file | Central tracking file checked into git; CI validates consistency |
| GLTCK-721 | Code annotations (`#[ignore]` with reason) | Distributed across test files; harder to get aggregate view |
| GLTCK-722 | Translator-driven | Unsupported determined at translation time by the Cypher-to-GQL translator |

**Recommendation**: GLTCK-722 primary, GLTCK-720 as output. The translator naturally identifies unsupported features during query translation. The manifest is generated/updated from test results, providing a single tracking file without manual annotation burden. When a feature is implemented, the translator starts producing `Translated` instead of `Unsupported`, and the test automatically moves from unsupported to pass/fail.

---

## 7. External Resources (with licenses)

| Resource | URL | License | Use |
|----------|-----|---------|-----|
| openCypher TCK 2024.2 | [github.com/opencypher/openCypher/tree/main/tck](https://github.com/opencypher/openCypher/tree/main/tck) | Apache-2.0 | Primary test scenarios |
| OpenGQL Grammar | [github.com/opengql/grammar](https://github.com/opengql/grammar) | Apache-2.0 | Grammar samples |
| Packt GQL Book | [github.com/PacktPublishing/Getting-Started-with-the-Graph-Query-Language-GQL](https://github.com/PacktPublishing/Getting-Started-with-the-Graph-Query-Language-GQL) | Public | GQL example queries |
| Neo4j Movies | [github.com/neo4j-graph-examples/movies](https://github.com/neo4j-graph-examples/movies) | Public | Test dataset |
| Northwind | [github.com/neo4j-graph-examples/northwind](https://github.com/neo4j-graph-examples/northwind) | Public domain | Test dataset |
| LDBC SNB | [ldbcouncil.org/benchmarks/snb/](https://ldbcouncil.org/benchmarks/snb/) | Apache-2.0 | Scale testing |
| OlofMorra GQL Parser | [github.com/OlofMorra/GQL-parser](https://github.com/OlofMorra/GQL-parser) | Apache-2.0 | Test queries |
| Neo4j GQL Conformance | [neo4j.com/docs/cypher-manual/current/appendix/gql-conformance/](https://neo4j.com/docs/cypher-manual/current/appendix/gql-conformance/) | Reference | Feature checklist |
| Ultipa GQL Conformance | [ultipa.com/docs/gql/gql-conformance](https://www.ultipa.com/docs/gql/gql-conformance) | Reference | Feature checklist |
| Apache AGE Tests | [github.com/apache/age](https://github.com/apache/age) | Apache-2.0 | Additional test patterns |
| cucumber-rs | [github.com/cucumber-rs/cucumber](https://github.com/cucumber-rs/cucumber) | Apache-2.0/MIT | Optional dependency |
| GRev | [github.com/CUHK-SE-Group/GRev](https://github.com/CUHK-SE-Group/GRev) | Public | Query rewriting reference |
| GDBMeter | [github.com/gdbmeter/gdbmeter](https://github.com/gdbmeter/gdbmeter) | Public | Predicate partitioning reference |

---

## 8. Success Metrics

| Metric | Phase 1 Target | Phase 2 Target | Phase 3 Target |
|--------|---------------|----------------|----------------|
| TCK scenarios tracked | 500+ | 1500+ | 2000+ |
| TCK pass rate | 30%+ | 50%+ | 70%+ |
| ISO mandatory features tested | 10+ | 20+ | All |
| Sample datasets integrated | 1 (Movies) | 2 (+ Northwind) | 3 (+ LDBC SF1) |
| Zero `fail` entries in manifest | Yes | Yes | Yes |
