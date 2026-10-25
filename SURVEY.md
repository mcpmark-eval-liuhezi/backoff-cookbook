# Retry with Exponential Backoff and Jitter: A Survey of Real Production Code

This page is the reference companion for an internal talk on how real
production codebases implement retry with exponential backoff and jitter.
Every entry was surfaced by live GitHub code search and then verified by
opening and reading the actual source file in the repository the search
pointed at. Nothing here is recalled from memory or invented, and nothing is
inferred from a file or class name alone.

The survey covers four ecosystems — **Python**, **Go**, **Java**, and
**JavaScript** — and fourteen distinct implementations. Each entry names the
repository, exactly where the code lives, what the code really does, and the
*jitter flavor* it chose, because that turns out to be the single most varied
decision in the whole design space: the same phrase "exponential backoff"
resolves, in production code, to full jitter, equal jitter, decorrelated
jitter, tight plus-or-minus 5%, wide plus-or-minus 25–33%, additive seconds,
and positive-only jitter.

---

## Table of contents

1. [Backoff as a Java `Stream` of durations — apache/bookkeeper](#1-apachebookkeeper-java)
2. [The AWS taxonomy as a class family — redis/lettuce](#2-redislettuce-java)
3. [Immutable builder with positive-only jitter — dgraph-io/dgraph4j](#3-dgraph-iodgraph4j-java)
4. [Composable middleware over one interface — sethvargo/go-retry](#4-sethvargogo-retry-go)
5. [Exponent clamping with tight ±5% jitter — Azure/iot-operations-sdks](#5-azureiot-operations-sdks-go)
6. [A ~40-line helper with ±25% jitter — interstellar/starlight](#6-interstellarstarlight-go)
7. [Strategy selection by name — luraproject/lura](#7-luraprojectlura-go)
8. [A fixed schedule plus additive jitter — inngest/inngest](#8-inngestinngest-go)
9. [Status-code-aware retry decorator — Blazemeter/taurus](#9-blazemetertaurus-python)
10. [Additive one-second jitter — helallao/perplexity-ai](#10-helallaoperplexity-ai-python)
11. [Jitter folded into the growth multiplier — MaartenGr/KeyBERT](#11-maartengrkeybert-python)
12. [Injectable full jitter for deterministic tests — yzs15/codex_proxy](#12-yzs15codex_proxy-python)
13. [A full strategy library behind a Protocol — dagdelenemre/nopanic](#13-dagdelenemrenopanic-python)
14. [Pluggable jitter strategies on an async retry class — hamr0/bareagent](#14-hamr0bareagent-javascript)

---

## 1. apache/bookkeeper (Java)

**Repository:** [apache/bookkeeper](https://github.com/apache/bookkeeper) —
"Apache BookKeeper - a scalable, fault tolerant and low latency storage
service optimized for append-only workloads."

**Where the code lives:**
[`bookkeeper-common/src/main/java/org/apache/bookkeeper/common/util/Backoff.java`](https://github.com/apache/bookkeeper/blob/master/bookkeeper-common/src/main/java/org/apache/bookkeeper/common/util/Backoff.java)

**What the code does:**

The most architecturally interesting entry: backoff is modeled not as a loop
but as a **`Stream<Long>` of durations**. A `Policy` interface exposes a
single method, `toBackoffs()`, that converts a policy into an infinite (or
length-limited) stream of wait times. Consumers pull the next value from the
stream whenever a retry happens — the policy itself stays stateless.

Three policies ship. `Constant` repeats one delay forever. `Exponential` is
literally `Stream.iterate(startMs, lastMs -> Math.min(lastMs * multiplier, maxMs))`.
The interesting one is `Jitter`, which takes a `Jitter.Type` enum of
`DECORRELATED`, `EQUAL`, or `EXPONENTIAL`; the `EXPONENTIAL` variant is
documented in its Javadoc as "full jitter" per the AWS Architecture Blog post,
with a link to that post. The class also defines
`DEFAULT = Jitter.of(Type.EXPONENTIAL, 200, 2000, 3)` — a 200 ms start, a 2 s
cap, and three attempts.

A subtle production detail: the jittered exponential path converts everything
to nanoseconds, tracks attempts in an `AtomicLong`, and clamps the bit shift
used to compute `2^attempts` at a constant `MaxBitShift = 62`, so a runaway
retry loop can never overflow a `long`. That defensive cap is easy to forget
and is exactly what separates production code from a tutorial.

**Jitter flavor:** full jitter, plus equal-jitter and decorrelated variants
behind the same interface.

---

## 2. redis/lettuce (Java)

**Repository:** [redis/lettuce](https://github.com/redis/lettuce) — "Redis
Java client."

**Where the code lives:** the `io.lettuce.core.resource` package —
[`Delay.java`](https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/resource/Delay.java)
(the abstract base and factory),
[`ExponentialDelay.java`](https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/resource/ExponentialDelay.java),
[`EqualJitterDelay.java`](https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/resource/EqualJitterDelay.java),
[`FullJitterDelay.java`](https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/resource/FullJitterDelay.java),
and
[`DecorrelatedJitterDelay.java`](https://github.com/redis/lettuce/blob/main/src/main/java/io/lettuce/core/resource/DecorrelatedJitterDelay.java).

**What the code does:**

Lettuce uses reconnect delays, and this package implements the complete AWS
jitter taxonomy as a small class family. `Delay` is an abstract base with one
method, `createDelay(long attempt)`, and doubles as a factory: `constant()`,
`exponential()`, and `equalJitter()` return preconfigured instances. Defaults
are a zero lower bound, a 30-second upper bound, powers of two, and
milliseconds.

`ExponentialDelay` is the shared arithmetic core. It computes
`2^(attempt-1)` with a fast bitwise path, guarded on both ends: attempts of
zero or less return zero "as a safeguard against underflow", and attempts of
63 or more return `Long.MAX_VALUE - 1` "as a safeguard against overflow in the
bitshift operation". Every result is then squeezed through `applyBounds()`,
which replaces anything below the lower bound or above the upper bound.

The three jitter subclasses each override `createDelay`:

- `EqualJitterDelay` — `randomBetween(0, base * calculatePowerOfTwo(attempt))`.
- `FullJitterDelay` — computes `temp = min(upper, base * 2^attempt)`, then
  `delay = temp / 2 + randomBetween(0, temp / 2)`; in other words the top half
  of the range is preserved and only the bottom half is randomized.
- `DecorrelatedJitterDelay` — the only *stateful* one: it implements
  `Delay.StatefulDelay`, keeps a `volatile long prevDelay` (volatile because
  "delays may be used by different threads"), and returns
  `min(cap, randomBetween(base, prevDelay * 3))`. A `reset()` method zeroes
  the state so the instance can be reused after a successful reconnect.

**Jitter flavor:** the whole taxonomy — equal, full, and decorrelated — each
as its own class with bounds applied uniformly.

---

## 3. dgraph-io/dgraph4j (Java)

**Repository:** [dgraph-io/dgraph4j](https://github.com/dgraph-io/dgraph4j) —
"Official Dgraph Java client."

**Where the code lives:**
[`src/main/java/io/dgraph/RetryPolicy.java`](https://github.com/dgraph-io/dgraph4j/blob/main/src/main/java/io/dgraph/RetryPolicy.java)

**What the code does:**

An immutable, builder-configured policy (instances are created via
`RetryPolicy.builder()`, and the builder validates its inputs — `jitter` must
be between 0 and 1, delays must be non-negative). The class Javadoc states
the formula outright:

```
min(baseDelay * 2^attempt, maxDelay) + random(0, delay * jitter)
```

The implementation is three lines:

```java
long delay = Math.min(base * (1L << attempt), max);
if (jitter > 0) {
  delay += (long) (delay * jitter * ThreadLocalRandom.current().nextDouble());
}
```

Defaults are 5 retries, a 100 ms base delay, a 5 s max delay, and 10% jitter.
Note what is unusual here: the jitter is **positive-only** and *additive* — the
computed delay is a floor, never reduced. That keeps the exponential curve
intact while still spreading clients out. Two convenience presets,
`readOnly()` and `bestEffort()`, wrap the builder for read-only transaction
semantics.

**Jitter flavor:** additive, positive-only, `+U(0, delay * jitter)`.

---

## 4. sethvargo/go-retry (Go)

**Repository:** [sethvargo/go-retry](https://github.com/sethvargo/go-retry) —
"Go library for retrying with configurable backoffs."

**Where the code lives:**
[`backoff.go`](https://github.com/sethvargo/go-retry/blob/main/backoff.go)

**What the code does:**

The cleanest *architecture* in the Go entries: a two-method interface,
`Backoff`, whose single meaningful method is `Next() (time.Duration, bool)` —
return the wait, and whether to stop. Everything else is a **wrapper that
decorates another `Backoff`**:

- `WithJitter(j, next)` — adds plus-or-minus `j` around the wrapped value. The
  randomization is done in unsigned arithmetic,
  `rand.Uint64N(uint64(j)*2) - uint64(j)`, precisely because "the unsigned
  bound fits even when twice j exceeds MaxInt64", then passed through
  `addClamp`, which pins the result to `[0, math.MaxInt64]`.
- `WithJitterPercent(j, next)` — same idea as a percentage; `j` is clamped to
  100 because "above 100 the computed percentage goes negative".
- `WithFullJitter(next)` — returns `rand.Int64N(val)`, i.e. uniform in
  `[0, val)`. The doc comment explicitly contrasts it with `WithJitter`:
  "full jitter spreads the wait across the entire range, which spreads out
  retrying clients more effectively", and links the AWS blog post.
- `WithMaxRetries(max, next)` — a mutex-guarded attempt counter that flips
  `stop` once the budget is spent.
- `WithCappedDuration(cap, next)` — caps any single value.
- `WithMaxDuration(timeout, next)` — caps the *total* elapsed time
  (best-effort, as its comment is careful to say).

Because these compose, `WithFullJitter(WithCappedDuration(30*time.Second,
Exponential(...)))` reads like a sentence. This decorator style is the most
directly portable pattern in the survey.

**Jitter flavor:** all of them, as interchangeable middleware — symmetric
absolute, symmetric percentage, and full jitter.

---

## 5. Azure/iot-operations-sdks (Go)

**Repository:** [Azure/iot-operations-sdks](https://github.com/Azure/iot-operations-sdks) —
"SDKs to support the Azure IoT Operations developer experience."

**Where the code lives:**
[`go/mqtt/retry/exponential_backoff.go`](https://github.com/Azure/iot-operations-sdks/blob/main/go/mqtt/retry/exponential_backoff.go)

**What the code does:**

An `ExponentialBackoff` struct configured with `MaxAttempts`, `MinInterval`
(defaulting to an eighth of a second), `MaxInterval` (defaulting to one
minute), a total `Timeout`, and a `NoJitter` escape hatch. `Start` runs the
retry loop: it wraps the caller's context in a timeout if one is set, invokes
the `Task`, and sleeps inside a `select` that races the sleep against
`ctx.Done()`, so cancellation always wins over a long backoff. All time reads
go through an injectable `wallclock.Instance`, which is what makes the loop
unit-testable.

The exponent calculation is the clever part. Rather than computing
`minInterval * 2^attempt` and then clamping the result, it clamps the
**exponent itself**:

```go
factor := math.Pow(2, min(
    float64(attempt-1),
    math.Log2(float64(maxInterval)/float64(minInterval)),
))
```

Once the attempt count exceeds `log2(max/min)`, the exponent stops growing
and the interval simply saturates — no overflow, no wasted multiplication.

Jitter here is deliberately **tight**: `jitter(base)` returns
`base * (.95 + .1*j)` for `j` in `[0,1)`, so the final delay lands between
95% and 105% of the computed interval. The helper seeds a fresh
`rand.New(rand.NewSource(wallclock.Instance.Now().UnixNano()))` on every call
and carries a `#nosec G404` comment acknowledging that a `math/rand` source is
fine for load shaping. A nice contrast with full jitter: Azure wants to avoid
synchronicity without giving up the predictable pacing of the curve.

**Jitter flavor:** ±5% multiplicative, on by default, removable via `NoJitter`.

---

## 6. interstellar/starlight (Go)

**Repository:** [interstellar/starlight](https://github.com/interstellar/starlight) —
"Payment channels on Stellar."

**Where the code lives:**
[`net/backoff.go`](https://github.com/interstellar/starlight/blob/master/net/backoff.go)

**What the code does:**

The polar opposite of the BookKeeper entry: the whole file is about forty
lines — a `Backoff` struct plus two functions. `Next()` grows the previous
delay by a constant multiplier `backoff = 1.2` each call, starting from `Base`
on the first call. One line deserves attention:

```go
dur2 := time.Duration(float64(dur) * backoff)
if dur2 == dur {
    dur2++
}
```

Because `time.Duration` is integer nanoseconds, multiplying a very small
duration by 1.2 can round back to the same value; the `dur2++` guarantees the
backoff always makes progress. A real bug avoided by one character.

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

## 7. luraproject/lura (Go)

**Repository:** [luraproject/lura](https://github.com/luraproject/lura) —
"Ultra performant API Gateway with middlewares. A project hosted at The Linux
Foundation."

**Where the code lives:**
[`backoff/backoff.go`](https://github.com/luraproject/lura/blob/master/backoff/backoff.go)

**What the code does:**

A strategy-selector design: `GetByName(strategy string)` maps a
configuration string to one of five implementations of a
`TimeToWaitBeforeRetry func(int) time.Duration` type — `linear`,
`linear-jitter`, `exponential`, `exponential-jitter`, or a fixed default.
`ExponentialBackoff(i)` is a one-liner, `time.Duration(1<<uint(i)) *
time.Second`. The interesting part is that jitter is factored into a single
shared helper used by both jittered variants:

```go
func jitter(i int) time.Duration {
    ms := i * 1000
    maxJitter := ms/3 + 1
    ms += random.Intn(2*maxJitter) - maxJitter
    if ms <= 0 {
        ms = 1
    }
    return time.Duration(ms) * time.Millisecond
}
```

So the jitter is ±33% of the base, computed in integer milliseconds, with a
final floor of 1 ms so the delay can never reach zero. The package also keeps
its own `rand.Rand` seeded from `time.Now().UnixNano()` in `init()`, avoiding
contention on the global source.

**Jitter flavor:** ±33% symmetric, shared by the linear and exponential
strategies, with a non-zero floor.

---

## 8. inngest/inngest (Go)

**Repository:** [inngest/inngest](https://github.com/inngest/inngest) —
"The leading workflow orchestration platform."

**Where the code lives:**
[`pkg/backoff/backoff.go`](https://github.com/inngest/inngest/blob/main/pkg/backoff/backoff.go)

**What the code does:**

Two different strategies in one small file. `ExponentialJitterBackoff`
computes `2^(attempt-1)`, multiplies by ten "to get 10 second breaks at
minimum", and applies 0–15% multiplicative jitter:

```go
backoff := float64(uint(1) << (uint(attemptNum) - 1))
backoff += backoff * (0.15 * rand.Float64())
backoff = backoff * 10
```

Once the result reaches twelve hours it stops growing and instead returns
twelve hours plus up to 120 seconds of random jitter — the cap itself is
jittered, so even saturated retries stay desynchronized.

`TableBackoff`, the package default, takes the opposite approach: a fixed
lookup table of ten durations (15 s, 30 s, 1 m, 2 m, 5 m, 10 m, 20 m, 40 m,
1 h, 2 h), clamped at the end of the table, plus **additive** jitter of 0–30
seconds:

```go
jitter := time.Duration(rand.Int31n(30_000)) * time.Millisecond
return time.Now().Add(at).Add(jitter)
```

A reminder that "exponential backoff" is often replaced in practice by an
explicit schedule, because a table is auditable and predictable.

**Jitter flavor:** 0–15% multiplicative on the exponential path; 0–30 s
additive on the table path; plus a jittered hard cap.

---

## 9. Blazemeter/taurus (Python)

**Repository:** [Blazemeter/taurus](https://github.com/Blazemeter/taurus) —
"Automation-friendly framework for Continuous Testing."

**Where the code lives:**
[`bzt/bza_retry.py`](https://github.com/Blazemeter/taurus/blob/master/bzt/bza_retry.py)

**What the code does:**

A decorator-based retry module for the BlazeMeter API client. A `RetryConfig`
holds `max_attempts`, `initial_delay`, `backoff_factor`, `max_delay`, and a
boolean `jitter` flag; the core arithmetic is:

```python
delay = self.initial_delay * (self.backoff_factor ** attempt)
delay = min(delay, self.max_delay)
if self.jitter:
    jitter_factor = random.uniform(0.75, 1.25)
    delay *= jitter_factor
```

The interesting part is not the arithmetic but the **classification layer**.
`should_retry()` inspects the raised exception against a `NETWORK_PROBLEMS`
tuple (`IOError`, `URLError`, `SSLError`, `ReadTimeout`,
`TaurusNetworkError`, `ProtocolError`, `ReadTimeoutError`) and against
explicit HTTP status lists — retrying 429 and 5xx, never retrying
400/401/403/404/405/406/409/410. It digs status codes out of several
exception shapes (`error.status_code`, `error.response.status_code`, and
`requests.RequestException` responses). A `transient=True` mode swallows the
final failure entirely for fire-and-forget operations, and convenience
presets `retry_once()` / `retry_aggressive()` wrap the main decorator.

**Jitter flavor:** symmetric multiplicative, ±25%, opt-in (default `False`).

---

## 10. helallao/perplexity-ai (Python)

**Repository:** [helallao/perplexity-ai](https://github.com/helallao/perplexity-ai) —
"Unofficial API Wrapper for Perplexity.ai."

**Where the code lives:**
[`perplexity/utils.py`](https://github.com/helallao/perplexity-ai/blob/master/perplexity/utils.py)

**What the code does:**

A `retry_with_backoff` decorator whose entire delay computation is one line:

```python
wait_time = backoff_factor**attempt + random.uniform(0, 1)
```

Unlike the multiplicative variants, this **adds** a random 0–1 s on top of the
pure exponential value, so the shape of the curve is preserved exactly and
only the collision-prone tail is softened. It is the only purely additive
jitter in the survey. The decorator also accepts an `on_retry(attempt,
exception)` callback invoked before sleeping — a simple and effective
observability hook — and the same file contains a `rate_limit` decorator that
sleeps `random.uniform(min_delay, max_delay)` between calls, a sibling
pattern for throttling rather than retrying.

**Jitter flavor:** additive, `+uniform(0, 1)` seconds.

---

## 11. MaartenGr/KeyBERT (Python)

**Repository:** [MaartenGr/KeyBERT](https://github.com/MaartenGr/KeyBERT) —
"Minimal keyword extraction with BERT."

**Where the code lives:**
[`keybert/llm/_utils.py`](https://github.com/MaartenGr/KeyBERT/blob/main/keybert/llm/_utils.py)

**What the code does:**

A private helper module for KeyBERT's LLM backends containing
`retry_with_exponential_backoff(func, initial_delay=1, exponential_base=2,
jitter=True, max_retries=10, errors=None)`, which returns a wrapper rather
than using the `@decorator` syntax — a functional rather than declarative
style. The delay update folds the randomness into the **growth multiplier**
instead of applying it to the final value:

```python
delay *= exponential_base * (1 + jitter * random.random())
```

So each retry multiplies the delay by `2 * (1 + U(0,1))`, i.e. between 2× and
4×, meaning the jitter compounds across attempts and the spread widens as the
backoff grows. `errors` is a tuple selecting which exception types are
retriable; anything else is re-raised immediately by the trailing
`except Exception as e: raise e`.

**Jitter flavor:** multiplicative, folded into the per-retry growth factor
(2–4× growth per attempt when enabled).

---

## 12. yzs15/codex_proxy (Python)

**Repository:** [yzs15/codex_proxy](https://github.com/yzs15/codex_proxy)

**Where the code lives:**
[`codex_proxy/backoff.py`](https://github.com/yzs15/codex_proxy/blob/main/codex_proxy/backoff.py)

**What the code does:**

A tiny, dependency-free `Backoff` dataclass in which **the jitter itself is
an injectable callable field**:

```python
def full_jitter(capped_delay: float) -> float:
    return random.uniform(0.0, capped_delay)

@dataclass
class Backoff:
    initial: float
    multiplier: float
    maximum: float
    jitter: Callable[[float], float] = full_jitter

    def ceiling(self, attempt: int) -> float:
        if attempt < 1:
            attempt = 1
        raw = self.initial * (self.multiplier ** (attempt - 1))
        return min(raw, self.maximum)

    def delay(self, attempt: int) -> float:
        return self.jitter(self.ceiling(attempt))
```

The separation of `ceiling()` (the deterministic cap) from `delay()` (where
randomness is applied) means a test can pass `jitter=lambda c: c` and get
perfectly deterministic capped-exponential behavior. The module docstring
states the intent outright: "Pure and dependency-free so it can be unit-tested
deterministically by injecting a fake `jitter` function." The cleanest
testability pattern in the survey, and directly borrowable.

**Jitter flavor:** full jitter (uniform in `[0, capped_delay]`), injectable.

---

## 13. dagdelenemre/nopanic (Python)

**Repository:** [dagdelenemre/nopanic](https://github.com/dagdelenemre/nopanic) —
"Unified resilience toolkit for Python: retry, circuit breaker, timeout, rate
limit, bulkhead, hedging. Zero deps, sync + async."

**Where the code lives:**
[`src/nopanic/backoff.py`](https://github.com/dagdelenemre/nopanic/blob/main/src/nopanic/backoff.py)

**What the code does:**

The most complete Python entry. Strategies are **frozen, slotted dataclasses
implementing a `Backoff` Protocol defined as "anything that can be iterated to
produce per-retry delays in seconds"** — iterating one yields the delay before
each successive retry:

```python
@dataclass(frozen=True, slots=True)
class full_jitter:
    base: float = 0.1
    factor: float = 2.0
    cap: float = 30.0

    def __iter__(self) -> Iterator[float]:
        delay = self.base
        while True:
            yield random.uniform(0.0, min(delay, self.cap))
            delay *= self.factor

@dataclass(frozen=True, slots=True)
class decorrelated_jitter:
    base: float = 0.1
    cap: float = 30.0

    def __iter__(self) -> Iterator[float]:
        delay = self.base
        while True:
            delay = min(self.cap, random.uniform(self.base, delay * 3))
            yield delay
```

Besides `full_jitter` and `decorrelated_jitter` it ships `fixed(delay)` and a
plain `exponential(base, factor, cap)` with no randomness at all, plus an
`as_backoff()` coercion that turns a bare number into a `fixed` strategy. The
docstring explains why the strategies are frozen dataclasses: they are "cheap,
hashable and safe to share between policies; each execution of a policy starts
a fresh iterator" — a direct fix for the state leak you would otherwise get
from sharing one iterator across concurrent retries. The module also has the
most thoughtful comment on RNG choice in the survey: jitter deliberately
uses the stdlib PRNG because "it shapes load, it is not a security boundary,
and a CSPRNG would add cost for no benefit", with the corresponding S311 lint
rule suppressed project-wide.

**Jitter flavor:** full jitter **and** decorrelated jitter as first-class,
interchangeable strategies behind one protocol.

---

## 14. hamr0/bareagent (JavaScript)

**Repository:** [hamr0/bareagent](https://github.com/hamr0/bareagent) —
"Gives agents a think→act loop... Zero required deps."

**Where the code lives:**
[`src/retry.js`](https://github.com/hamr0/bareagent/blob/main/src/retry.js)

**What the code does:**

An async `Retry` class whose options cover the full decision space:
`maxAttempts` (default 3), `backoff` (a fixed number of milliseconds,
`'linear'`, or `'exponential'`), a per-attempt `timeout`, a `retryOn`
predicate, and `jitter` accepting `false`, a number, `'full'`, or `'equal'`.
The default `retryOn` predicate is the interesting production detail — it
checks an explicit `err.retryable` flag first, then HTTP status (429, or 500
through 504), then socket error codes `ECONNRESET`, `ETIMEDOUT`, `ENOTFOUND`:

```js
const DEFAULT_RETRY_ON = (err) => {
  if (err.retryable === true) return true;
  if (err.retryable === false) return false;
  const status = err.status || err.statusCode;
  if (status === 429 || (status >= 500 && status <= 504)) return true;
  const code = err.code;
  if (code === 'ECONNRESET' || code === 'ETIMEDOUT' || code === 'ENOTFOUND') return true;
  return false;
};
```

`call()` races the user's promise against a `setTimeout` rejection to enforce
the per-attempt timeout, and clears the timer in `finally`. The base delay is
`Math.min(2 ** (attempt - 1) * 1000, 30000)`, and jitter is applied by a
strategy-dispatching helper:

```js
_applyJitter(base) {
  if (this.jitter === false || this.jitter === 0) return base;
  if (this.jitter === 'full')  return Math.floor(Math.random() * base);
  if (this.jitter === 'equal') return Math.floor(base / 2 + Math.random() * (base / 2));
  if (typeof this.jitter === 'number') {
    const spread = base * this.jitter;
    return Math.floor(base - spread + Math.random() * spread);
  }
  return base;
}
```

**Jitter flavor:** pluggable — none, full, equal, or a caller-chosen fraction.

---

## Queries used

Every search below was run against GitHub public code search. Result counts
are as reported at the time the search was executed; GitHub's counts move
slightly between runs. The first block is the initial language sweep, the
second block the refinement passes that widened the candidate pool and
surfaced more distinct jitter shapes.

1. `exponential backoff jitter` — 387,494 results
2. `exponential backoff jitter language:python` — 138,717 results
3. `exponential backoff jitter language:go` — 29,248 results
4. `exponential backoff jitter language:java` — 8,224 results
5. `retry backoff jitter language:javascript` — 28,768 results
6. `"exponential backoff" jitter language:Python` — 112,689 results
7. `"exponential backoff" jitter language:Go` — 19,432 results
8. `"exponential backoff" jitter language:Java` — 4,904 results
9. `retry jitter backoff language:JavaScript` — 28,768 results
10. `"full jitter" backoff language:python` — 36,352 results
11. `retry_with_exponential_backoff language:python` — 2,936 results
12. `DecorrelatedJitter language:java` — 99 results
13. `func ExponentialJitterBackoff language:go` — 220 results
14. `min max backoff random.uniform retry language:python` — 104,052 results
15. `sleep random.uniform backoff retries language:Python` — 79,872 results
16. `math.Pow(2, float64 retry) jitter language:Go` — 190 results
17. `"full jitter" retry language:Java` — 510 results
18. `"backoff" "jitter" setTimeout Math.random language:JavaScript` — 19,424 results
19. `backoff_factor random.uniform language:Python` — 20,608 results
20. `"retry_after" "2 **" "random.uniform" language:Python` — 20,942 results
21. `repo:dgraph-io/dgraph4j jitter backoff` — 3 results (repo-scoped verification pass)

Queries 1–5 and 10–14 produced the entries above; queries 6–9 and 15–20 were
additional phrasings used to widen the candidate pool and to confirm that the
jitter flavors represented here (full, equal, decorrelated, tight percentage,
wide percentage, additive) are each common enough in the wild to be worth a
slide. Every file that ended up in this survey was then opened and read in
full; candidates that turned out to be false positives — documentation
changelogs, TODO notes, vendored copies, or code whose name promised more
than it delivered — were dropped.
