# Appendix: Python Examples

A companion to [`SURVEY.md`](https://github.com/mcpmark-eval-liuhezi/backoff-cookbook/blob/main/SURVEY.md),
collecting every Python implementation found by the GitHub code searches. Each
file was opened and read in full; the notes below describe what the code
actually does.

Five distinct Python approaches turned up, and they map to five different
jitter flavors: symmetric multiplicative, additive, jitter folded into the
growth multiplier, injectable full jitter, and a full strategy library
containing both full and decorrelated jitter.

---

## 1. Blazemeter/taurus — status-code-aware retry decorator

**Repository:** [Blazemeter/taurus](https://github.com/Blazemeter/taurus)
("Automation-friendly framework for Continuous Testing")

**File:** [`bzt/bza_retry.py`](https://github.com/Blazemeter/taurus/blob/master/bzt/bza_retry.py)

**Approach:** decorator with a `RetryConfig` object; jitter is a boolean flag.

**Core delay code:**

```python
def calculate_delay(self, attempt: int) -> float:
    """Calculate delay for given attempt number (0-indexed)"""
    delay = self.initial_delay * (self.backoff_factor ** attempt)
    delay = min(delay, self.max_delay)

    if self.jitter:
        import random
        jitter_factor = random.uniform(0.75, 1.25)
        delay *= jitter_factor

    return delay
```

**What makes it worth reading:** the interesting part is not the arithmetic
but the classification layer. `should_retry()` inspects the exception against
`NETWORK_PROBLEMS` (`IOError`, `URLError`, `SSLError`, `ReadTimeout`,
`TaurusNetworkError`, `ProtocolError`, `ReadTimeoutError`) and against explicit
HTTP status lists — retrying 429 and 5xx, never retrying
400/401/403/404/405/406/409/410. It digs status codes out of several exception
shapes (`error.status_code`, `error.response.status_code`, and
`requests.RequestException` responses). A `transient=True` mode swallows the
final failure entirely for fire-and-forget operations, and convenience
presets `retry_once()` / `retry_aggressive()` wrap the main decorator.

**Jitter flavor:** symmetric multiplicative, ±25%, opt-in (default `False`).

---

## 2. helallao/perplexity-ai — additive one-second jitter

**Repository:** [helallao/perplexity-ai](https://github.com/helallao/perplexity-ai)
("Unofficial API Wrapper for Perplexity.ai")

**File:** [`perplexity/utils.py`](https://github.com/helallao/perplexity-ai/blob/master/perplexity/utils.py)

**Approach:** a `retry_with_backoff` decorator with an `on_retry` callback for
observability.

**Core delay code:**

```python
wait_time = backoff_factor**attempt + random.uniform(0, 1)
```

**What makes it worth reading:** unlike the multiplicative variants, this
**adds** a random 0–1 s on top of the pure exponential value, so the shape of
the curve is preserved exactly and only the collision-prone tail is softened.
This is the only purely additive jitter in the whole survey. The decorator also
supports an `on_retry(attempt, exception)` callback invoked before sleeping,
which is a simple and effective observability hook. The same file contains a
`rate_limit` decorator that sleeps `random.uniform(min_delay, max_delay)`
between calls — a sibling pattern for throttling rather than retrying.

**Jitter flavor:** additive, `+uniform(0, 1)` seconds.

---

## 3. MaartenGr/KeyBERT — jitter folded into the growth multiplier

**Repository:** [MaartenGr/KeyBERT](https://github.com/MaartenGr/KeyBERT)
("Minimal keyword extraction with BERT")

**File:** [`keybert/llm/_utils.py`](https://github.com/MaartenGr/KeyBERT/blob/main/keybert/llm/_utils.py)

**Approach:** a function-returning-a-wrapper helper (rather than `@decorator`
syntax) used by KeyBERT's LLM backends.

**Core delay code:**

```python
def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 10,
    errors: tuple = None,
):
    """Retry a function with exponential backoff."""

    def wrapper(*args, **kwargs):
        num_retries = 0
        delay = initial_delay

        while True:
            try:
                return func(*args, **kwargs)

            except errors:
                num_retries += 1

                if num_retries > max_retries:
                    raise Exception(f"Maximum number of retries ({max_retries}) exceeded.")

                delay *= exponential_base * (1 + jitter * random.random())
                time.sleep(delay)

            except Exception as e:
                raise e

    return wrapper
```

**What makes it worth reading:** the randomness is folded into the **growth
multiplier** instead of being applied to the final value, so each retry
multiplies the delay by `2 * (1 + U(0,1))` — between 2× and 4×. The jitter
therefore compounds across attempts and the spread widens as the backoff
grows, which is a genuinely different shape from either multiplicative-final
or additive jitter. Note also that `errors` is a tuple selecting which
exception types are retriable; anything else is re-raised immediately.

**Jitter flavor:** multiplicative, folded into the per-retry growth factor.

---

## 4. yzs15/codex_proxy — injectable full jitter

**Repository:** [yzs15/codex_proxy](https://github.com/yzs15/codex_proxy)

**File:** [`codex_proxy/backoff.py`](https://github.com/yzs15/codex_proxy/blob/main/codex_proxy/backoff.py)

**Approach:** a tiny, dependency-free `Backoff` dataclass where the jitter
itself is an injectable callable field.

**Core delay code:**

```python
def full_jitter(capped_delay: float) -> float:
    """AWS-style full jitter: sleep a random amount in ``[0, capped_delay]``."""
    return random.uniform(0.0, capped_delay)

@dataclass
class Backoff:
    initial: float
    multiplier: float
    maximum: float
    # Maps the deterministically-computed ceiling to the actual sleep duration.
    # Overridable in tests to remove randomness.
    jitter: Callable[[float], float] = full_jitter

    def ceiling(self, attempt: int) -> float:
        """Undithered delay ceiling for ``attempt`` (1 == first retry)."""
        if attempt < 1:
            attempt = 1
        raw = self.initial * (self.multiplier ** (attempt - 1))
        return min(raw, self.maximum)

    def delay(self, attempt: int) -> float:
        """Actual (jittered) delay to sleep before ``attempt``."""
        return self.jitter(self.ceiling(attempt))
```

**What makes it worth reading:** the separation of `ceiling()` (the
deterministic cap) from `delay()` (where randomness is applied) means a test
can pass `jitter=lambda c: c` and get perfectly deterministic capped
exponential behavior. The module docstring states the intent outright: "Pure
and dependency-free so it can be unit-tested deterministically by injecting a
fake `jitter` function." This is the cleanest testability pattern found
anywhere in the survey and is directly borrowable.

**Jitter flavor:** full jitter (uniform in `[0, capped_delay]`), injectable.

---

## 5. dagdelenemre/nopanic — a full strategy library

**Repository:** [dagdelenemre/nopanic](https://github.com/dagdelenemre/nopanic)
("Unified resilience toolkit for Python: retry, circuit breaker, timeout, rate
limit, bulkhead, hedging. Zero deps, sync + async.")

**File:** [`src/nopanic/backoff.py`](https://github.com/dagdelenemre/nopanic/blob/main/src/nopanic/backoff.py)

**Approach:** strategies are frozen, slotted dataclasses that implement a
`Backoff` **Protocol** defined as "any iterable of floats" — iterating one
yields the delay in seconds before each successive retry.

**Core strategy code:**

```python
@dataclass(frozen=True, slots=True)
class full_jitter:
    """Exponential growth with full jitter: uniform(0, min(cap, base*factor**n))."""

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
    """Decorrelated jitter: each delay is uniform(base, previous*3), capped."""

    base: float = 0.1
    cap: float = 30.0

    def __iter__(self) -> Iterator[float]:
        delay = self.base
        while True:
            delay = min(self.cap, random.uniform(self.base, delay * 3))
            yield delay
```

**What makes it worth reading:** this is the most complete Python entry.
Besides `full_jitter` and `decorrelated_jitter` it also ships `fixed(delay)`
and a plain `exponential(base, factor, cap)` with no randomness at all, so the
same interface covers the entire spectrum from deterministic to
randomized-and-stateful. The docstring notes why the strategies are frozen
dataclasses: "cheap, hashable and safe to share between policies; each
execution of a policy starts a fresh iterator" — a direct fix for the state
leak you'd otherwise get from sharing one iterator across concurrent retries.
The module also contains the most thoughtful comment on RNG choice in the
survey: jitter deliberately uses the stdlib PRNG because "it shapes load, it
is not a security boundary, and a CSPRNG would add cost for no benefit," with
the corresponding S311 lint rule suppressed project-wide.

**Jitter flavor:** full jitter **and** decorrelated jitter, as first-class
interchangeable strategies behind one protocol.
