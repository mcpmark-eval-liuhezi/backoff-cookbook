# Retry with Exponential Backoff and Jitter: A Survey of Real Production Code

This page is a reference companion for an internal talk on how real codebases
implement retry with exponential backoff and jitter. Every entry below was
found through live GitHub code search and verified by opening the actual
source file — nothing here is recalled from memory or invented. Code snippets
are summarized; follow the links to read the originals in context.

The survey covers four ecosystems: **Python**, **Go**, **Java**, and
**JavaScript**. Where interesting, the entries call out the *jitter flavor*
each implementation chose, because that turns out to be the most varied
decision in the whole design space.

---

## Table of contents

1. [The AWS taxonomy, implemented as a Java `Stream` — apache/bookkeeper](#1-apachebookkeeper-java)
2. [Exponent clamping with tight ±5% jitter — Azure/iot-operations-sdks](#2-azureiot-operations-sdks-go)
3. [A ~50-line retry helper with ±25% jitter — interstellar/starlight](#3-interstellarstarlight-go)
4. [Status-code-aware retry decorator — Blazemeter/taurus](#4-blazemetertaurus-python)
5. [Additive one-second jitter — helallao/perplexity-ai](#5-helallaoperplexity-ai-python)
6. [Injectable full jitter for deterministic tests — yzs15/codex_proxy](#6-yzs15codex_proxy-python)
7. [A full strategy library: fixed / exponential / full / decorrelated — dagdelenemre/nopanic](#7-dagdelenemrenopanic-python)
8. [Pluggable jitter strategies on an async retry class — hamr0/bareagent](#8-hamr0bareagent-javascript)
9. [Immutable builder with positive-only jitter — dgraph-io/dgraph4j](#9-dgraph-iodgraph4j-java)

---

## 1. apache/bookkeeper (Java)

**Repository:** [apache/bookkeeper](https://github.com/apache/bookkeeper) —
"Apache BookKeeper - a scalable, fault tolerant and low latency storage service
optimized for append-only workloads."

**Where the code lives:**
[`bookkeeper-common/src/main/java/org/apache/bookkeeper/common/util/Backoff.java`](https://github.com/apache/bookkeeper/blob/master/bookkeeper-common/src/main/java/org/apache/bookkeeper/common/util/Backoff.java)

**What the code does:**

This is the most architecturally interesting entry: backoff is modeled not as
a loop but as a **`Stream<Long>` of durations**. A `Policy` interface exposes a
single method, `toBackoffs()`, that converts a policy into an infinite (or
length-limited) stream of wait times. Consumers just pull the next value from
the stream whenever a retry happens — the policy itself is stateless.

Three policies ship: `Constant` (the same delay forever), `Exponential`
(`Stream.iterate(startMs, last -> Math.min(last * multiplier, maxMs))`), and
the interesting one, `Jitter`, which takes a `Jitter.Type` enum of
`DECORRELATED`, `EQUAL`, or `EXPONENTIAL`. The `EXPONENTIAL` variant is
explicitly documented as "full jitter" per the AWS Architecture Blog
post, and the Javadoc links to that post directly. The class also defines
`DEFAULT = Jitter.of(Type.EXPONENTIAL, 200, 2000, 3)` — a 200 ms start, a 2 s
cap, and 3 attempts.

A subtle production detail: the jittered exponential path converts everything
to nanoseconds, tracks attempts in an `AtomicLong`, and clamps the bit-shift
used to compute `2^attempts` at a constant `MaxBitShift = 62` so a runaway
retry loop can never overflow a `long`. That kind of defensive cap is easy to
forget and is exactly what distinguishes production code from a tutorial.

**Jitter flavor:** full jitter (uniform in `[start, min(cap, start * 2^n])`),
plus equal-jitter and decorrelated variants behind the same interface.

---

## 2. Azure/iot-operations-sdks (Go)

**Repository:** [Azure/iot-operations-sdks](https://github.com/Azure/iot-operations-sdks) —
"SDKs to support the Azure IoT Operations developer experience."

**Where the code lives:**
[`go/mqtt/retry/exponential_backoff.go`](https://github.com/Azure/iot-operations-sdks/blob/main/go/mqtt/retry/exponential_backoff.go)

**What the code does:**

An `ExponentialBackoff` struct configured with `MaxAttempts`, `MinInterval`
(defaulting to `time.Second / 8`), `MaxInterval` (defaulting to one minute), a
total `Timeout`, and a `NoJitter` escape hatch. The `Start` method runs the
retry loop: it wraps the caller's context in a timeout if one is set, invokes a
`Task` function, and sleeps in a `select` that races the sleep against
`ctx.Done()` so cancellation always wins. All time reads go through an
injectable `wallclock.Instance`, which is what makes the retry loop unit
testable.

The exponent calculation is the clever part. Rather than computing
`minInterval * 2^attempt` and then clamping, it clamps the **exponent itself**:

```go
factor := math.Pow(2, min(
    float64(attempt-1),
    math.Log2(float64(maxInterval)/float64(minInterval)),
))
```

Once the attempt count exceeds `log2(max/min)`, the exponent stops growing and
the interval simply saturates at `MaxInterval` — no overflow, no wasted
multiplication.

Jitter here is deliberately **tight**: a `jitter(base)` helper returns
`base * (.95 + .1*j)` where `j` is a `float64` in `[0,1)`, i.e. the final delay
lands between 95% and 105% of the computed interval. The code seeds a fresh
`rand.New(rand.NewSource(wallclock.Instance.Now().UnixNano()))` per call and
carries a `#nosec G404` comment acknowledging that a math/rand source is fine
for load shaping. This is a nice contrast to full jitter: Azure wants to *avoid
synchronicity* without giving up the predictable pacing of the exponential
curve.

**Jitter flavor:** ±5% multiplicative, on by default, removable via `NoJitter`.

---

## 3. interstellar/starlight (Go)

**Repository:** [interstellar/starlight](https://github.com/interstellar/starlight) —
"Payment channels on Stellar."

**Where the code lives:**
[`net/backoff.go`](https://github.com/interstellar/starlight/blob/master/net/backoff.go)

**What the code does:**

The polar opposite of the BookKeeper entry: the entire file is about 40 lines
and contains just a `Backoff` struct plus two functions. `Next()` grows the
previous delay by a constant multiplier `backoff = 1.2` each call, starting
from `Base` on the first call. One line deserves attention:

```go
dur2 := time.Duration(float64(dur) * backoff)
if dur2 == dur {
    dur2++
}
```

Because `time.Duration` is integer nanoseconds, multiplying a very small
duration by 1.2 can round back to the same value; the `dur2++` guarantees the
backoff always makes progress. That is a real bug avoided by one character.

`Jitter(dur)` then randomizes the result into roughly `dur ± 25%`:

```go
h := int64(dur / 2)
delta := rand.Int63n(h) - h/2
return dur + time.Duration(delta)
```

A good example of the "small, self-contained helper you can paste into any
project" school of backoff — worth showing in a talk precisely because it is
readable in thirty seconds.

**Jitter flavor:** ±25% symmetric multiplicative.

---

## 4. Blazemeter/taurus (Python)

**Repository:** [Blazemeter/taurus](https://github.com/Blazemeter/taurus) —
"Automation-friendly framework for Continuous Testing."

**Where the code lives:**
[`bzt/bza_retry.py`](https://github.com/Blazemeter/taurus/blob/master/bzt/bza_retry.py)

**What the code does:**

A decorator-based retry module for the BlazeMeter API client. A `RetryConfig`
dataclass holds `max_attempts`, `initial_delay`, `backoff_factor`,
`max_delay`, and a boolean `jitter` flag. The core math is:

```python
delay = self.initial_delay * (self.backoff_factor ** attempt)
delay = min(delay, self.max_delay)
if self.jitter:
    jitter_factor = random.uniform(0.75, 1.25)
    delay *= jitter_factor
```

The interesting part is not the arithmetic but the **retry classification**.
The module distinguishes `NETWORK_PROBLEMS` (`IOError`, `URLError`,
`SSLError`, `ReadTimeout`, `ProtocolError`, `ReadTimeoutError`, and a
`TaurusNetworkError`) from HTTP status codes, retrying 429 and 5xx while
explicitly *not* retrying 400/401/403/404/409/410. It extracts status codes
from several different exception shapes (`error.status_code`,
`error.response.status_code`, `requests.RequestException`). A `transient=True`
mode swallows the final failure entirely for operations like KPI upload where
giving up silently is acceptable, and the module ships convenience presets
(`retry_once`, `retry_aggressive`) on top of the main decorator. This is a
realistic picture of where the complexity actually lives in production retry
code: in *deciding what to retry*, not in the delay formula.

**Jitter flavor:** symmetric multiplicative ±25%, opt-in.

---

## 5. helallao/perplexity-ai (Python)

**Repository:** [helallao/perplexity-ai](https://github.com/helallao/perplexity-ai) —
"Unofficial API Wrapper for Perplexity.ai."

**Where the code lives:**
[`perplexity/utils.py`](https://github.com/helallao/perplexity-ai/blob/master/perplexity/utils.py)

**What the code does:**

A `retry_with_backoff` decorator that is worth including because its jitter
line is a different shape from everything else in this survey:

```python
wait_time = backoff_factor**attempt + random.uniform(0, 1)
```

Rather than scaling the whole delay, it **adds** a uniform random 0–1 seconds
on top of the pure exponential value. This is the "additive jitter" flavor: it
keeps the exponential curve intact and only decorrelates clients that are
about to collide, at the cost of relatively weak smoothing at large delays.
The decorator also supports an `on_retry` callback invoked with the attempt
number and the exception, which is a nice observability hook, and it logs each
retry with the computed wait time. The same file contains an unrelated
`rate_limit` decorator that sleeps a random `uniform(min_delay, max_delay)`
between calls — a sibling pattern for throttling rather than retrying.

**Jitter flavor:** additive, `+uniform(0, 1)` seconds.

---

## 6. yzs15/codex_proxy (Python)

**Repository:** [yzs15/codex_proxy](https://github.com/yzs15/codex_proxy)

**Where the code lives:**
[`codex_proxy/backoff.py`](https://github.com/yzs15/codex_proxy/blob/main/codex_proxy/backoff.py)

**What the code does:**

A deliberately tiny, dependency-free module whose docstring states the design
goal outright: "Pure and dependency-free so it can be unit-tested
deterministically by injecting a fake `jitter` function." It implements AWS
full jitter as a standalone function:

```python
def full_jitter(capped_delay: float) -> float:
    """AWS-style full jitter: sleep a random amount in ``[0, capped_delay]``."""
    return random.uniform(0.0, capped_delay)
```

A `Backoff` dataclass holds `initial`, `multiplier`, `maximum`, and — the
interesting bit — a field `jitter: Callable[[float], float] = full_jitter`.
The class separates `ceiling(attempt)` (the undithered cap,
`min(initial * multiplier ** (attempt - 1), maximum)`) from
`delay(attempt)` (which is `self.jitter(self.ceiling(attempt))`). Because the
randomness is isolated behind that callable, a test can pass
`jitter=lambda c: c` and turn the strategy into plain capped exponential
backoff. This separation of "compute the cap" from "apply the randomness" is
the cleanest testability pattern in the survey and is easy to borrow.

**Jitter flavor:** full jitter, injectable.

---

## 7. dagdelenemre/nopanic (Python)

**Repository:** [dagdelenemre/nopanic](https://github.com/dagdelenemre/nopanic) —
"Unified resilience toolkit for Python: retry, circuit breaker, timeout, rate
limit, bulkhead, hedging. Zero deps, sync + async."

**Where the code lives:**
[`src/nopanic/backoff.py`](https://github.com/dagdelenemre/nopanic/blob/main/src/nopanic/backoff.py)

**What the code does:**

The most complete Python entry. A `Backoff` is defined as a `Protocol`: "any
iterable of floats: iterating it yields the delay (in seconds) to sleep before
each successive retry." Strategies are frozen, slotted dataclasses, which the
docstring notes makes them "cheap, hashable and safe to share between
policies; each execution of a policy starts a fresh iterator."

Four strategies are provided:

- `fixed(delay)` — the same value forever.
- `exponential(base, factor, cap)` — deterministic growth, capped:
  `yield min(delay, self.cap); delay *= self.factor`.
- `full_jitter(base, factor, cap)` — the recommended default, yielding
  `random.uniform(0.0, min(delay, self.cap))`, explicitly cited to the AWS
  Architecture Blog post "Exponential Backoff and Jitter" (2015) and
  described as the guard against "retry stampedes."
- `decorrelated_jitter(base, cap)` — the stateful variant, where each delay
  depends on the previous one:
  `delay = min(self.cap, random.uniform(self.base, delay * 3))`.

The module docstring also contains a thoughtful note on random-number choice
that is unusual to see spelled out in code: jitter deliberately uses the
stdlib PRNG because "it shapes load, it is not a security boundary, and a
CSPRNG would add cost for no benefit," and it suppresses the corresponding
S311 lint rule project-wide for that reason.

**Jitter flavor:** full jitter and decorrelated jitter as first-class,
interchangeable strategies.

---

## 8. hamr0/bareagent (JavaScript)

**Repository:** [hamr0/bareagent](https://github.com/hamr0/bareagent) —
"Gives agents a think→act loop… Zero required deps."

**Where the code lives:**
[`src/retry.js`](https://github.com/hamr0/bareagent/blob/main/src/retry.js)

**What the code does:**

A `Retry` class whose async `call(fn)` wraps each attempt in a
`Promise.race` against a `setTimeout`-based `TimeoutError`, so a single
attempt can be killed independently of the overall retry budget — a per-attempt
timeout, which none of the other entries combine with backoff in quite this
way.

`_delay(attempt)` picks a base: a fixed number of milliseconds, a `linear`
mode (`attempt * 1000`), or the default exponential
`Math.min(2 ** (attempt - 1) * 1000, 30000)` capped at 30 s. Then `_applyJitter`
dispatches on the configured jitter option, effectively giving this file the
widest jitter vocabulary in the survey in one place:

```js
if (this.jitter === 'full')  return Math.floor(Math.random() * base);
if (this.jitter === 'equal') return Math.floor(base / 2 + Math.random() * (base / 2));
if (typeof this.jitter === 'number') {
    const spread = base * this.jitter;
    return Math.floor(base - spread + Math.random() * spread);
}
```

A default `retryOn` predicate decides what to retry at all — `429`,
`500–504`, and the Node error codes `ECONNRESET`, `ETIMEDOUT`, `ENOTFOUND` —
with an escape hatch where an error carrying `retryable: true/false` overrides
the status-code check. `maxAttempts`, `retryOn`, and `timeout` can all be
overridden per call rather than only at construction.

**Jitter flavor:** selectable — full, equal, or a fractional spread — plus
linear/exponential/fixed backoff, all runtime-pluggable.

---

## 9. dgraph-io/dgraph4j (Java)

**Repository:** [dgraph-io/dgraph4j](https://github.com/dgraph-io/dgraph4j) —
"Official Dgraph Java client."

**Where the code lives:**
[`src/main/java/io/dgraph/RetryPolicy.java`](https://github.com/dgraph-io/dgraph4j/blob/main/src/main/java/io/dgraph/RetryPolicy.java)

**What the code does:**

An immutable, `Duration`-based policy built through a fluent `Builder` with
validation (`maxRetries` must be ≥ 0, `jitter` must be between 0 and 1). The
class Javadoc states the formula explicitly:
`min(baseDelay * 2^attempt, maxDelay) + random(0, delay * jitter)`, and the
implementation matches:

```java
long delay = Math.min(base * (1L << attempt), max);
if (jitter > 0) {
    delay += (long) (delay * jitter * ThreadLocalRandom.current().nextDouble());
}
```

Defaults are 5 retries, a 100 ms base, a 5 s cap, and 10% jitter. Two details
stand out. First, the jitter is **additive and positive-only** — it can only
lengthen a delay, never shorten it, so the exponential lower bound is
guaranteed. Second, `ThreadLocalRandom` is used rather than a shared `Random`
instance, which matters under concurrent load because a contended `Random`
becomes a serialization point across retrying threads. The policy is also
domain-aware: builders exist for `readOnly()` and `bestEffort()` transactions,
showing backoff configuration being tied to transaction semantics rather than
left as a transport-level detail.

**Jitter flavor:** additive, positive-only, up to 10% of the capped delay.

---

## Patterns worth calling out in a talk

Across these nine entries the *delay curve* is nearly always the same —
`initial * multiplier^attempt`, capped — and the genuinely divergent design
decisions are:

- **Jitter shape.** Full jitter (`uniform(0, cap)`) appears in bookkeeper,
  codex_proxy, nopanic, and bareagent's `'full'` mode; symmetric multiplicative
  (±25%) in starlight and taurus; tight ±5% in Azure's SDK; additive
  `+uniform(0,1)` in perplexity-ai; additive positive-only in dgraph4j; and
  decorrelated (previous-delay-dependent) in bookkeeper and nopanic.
- **Statefulness.** BookKeeper and nopanic make the policy a pure stream or
  iterator; starlight and decorrelated jitter must remember the last delay.
- **Testability.** codex_proxy injects the jitter function; Azure injects a
  clock; nopanic relies on fresh iterators from frozen dataclasses.
- **Overflow safety.** BookKeeper clamps its bit-shift at 62; Azure clamps
  the exponent itself at `log2(max/min)`.
- **Retry classification.** taurus and bareagent encode HTTP/errno
  allow-lists; dgraph4j ties the policy to transaction modes.

---

## Queries used

The following exact GitHub code searches were run to produce this survey.
Result counts are as reported by GitHub at the time of the search.

1. `"exponential backoff" jitter language:Python` — 112,689 results
2. `"exponential backoff" jitter language:Go` — 19,432 results
3. `"exponential backoff" jitter language:Java` — 4,904 results
4. `retry jitter backoff language:JavaScript` — 28,768 results
5. `sleep random.uniform backoff retries language:Python` — 79,872 results
6. `math.Pow(2, float64 retry) jitter language:Go` — 190 results
7. `"full jitter" retry language:Java` — 510 results
8. `"backoff" "jitter" setTimeout Math.random language:JavaScript` — 19,424 results
9. `backoff_factor random.uniform language:Python` — 20,608 results
10. `"retry_after" "2 **" "random.uniform" language:Python` — 20,942 results
11. `"full jitter" backoff language:Python` — 36,352 results

Candidate files surfaced by these searches were opened and read in full before
being included; entries that turned out to be unrelated to retry (for example
a large general-purpose `util.js` that merely mentioned the words) were
dropped rather than described from their filenames alone.
