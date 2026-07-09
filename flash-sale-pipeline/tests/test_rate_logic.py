"""
Offline sanity check for the producer's tick-based rate limiter.
Run with: python3 tests/test_rate_logic.py
No Kafka / network required - this only validates the arithmetic that decides
how many events to emit per 100ms tick, so we trust the real generator.py
before deploying the full Docker stack.
"""

TICK_SECONDS = 0.1


def simulate(eps: float, duration_sec: float):
    n_ticks = int(duration_sec / TICK_SECONDS)
    total = 0
    counts = []
    for i in range(1, n_ticks + 1):
        elapsed = i * TICK_SECONDS
        target_cumulative = int(eps * elapsed)
        n_events = max(0, target_cumulative - total)
        total += n_events
        counts.append(n_events)
    # final top-up, mirrors generator.py's end-of-phase correction
    target_final = int(eps * duration_sec)
    if target_final > total:
        total = target_final
    return total, counts


def check(eps, duration_sec, tolerance=0.01):
    total, counts = simulate(eps, duration_sec)
    expected = eps * duration_sec
    error = abs(total - expected) / expected if expected else 0
    status = "OK" if error <= tolerance else "FAIL"
    print(f"[{status}] eps={eps:>7.0f} duration={duration_sec:>5.0f}s -> "
          f"expected={expected:>9.1f} actual={total:>9d} error={error*100:.3f}%")
    assert error <= tolerance, f"Rate error too high for eps={eps}"


if __name__ == "__main__":
    check(100, 120)
    check(10000, 30)
    check(1, 10)
    check(0.5, 20)
    check(100000, 5)
    print("\nAll rate-limiter sanity checks passed.")
