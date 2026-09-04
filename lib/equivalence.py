"""Equivalence harness: prove candidate == baseline. Cheapest checks first; RAISE on divergence."""


def check_counts(spark, baseline: str, candidate: str, partition_cols=None) -> dict:
    """Total + per-partition row counts must match."""
    raise NotImplementedError


def check_fingerprint(spark, baseline: str, candidate: str) -> dict:
    """Per-column sum/min/max/count-distinct/null-count must match."""
    raise NotImplementedError


def check_except_all(spark, baseline: str, candidate: str, epsilon: float = 1e-6) -> dict:
    """A EXCEPT ALL B and B EXCEPT ALL A both empty (order/dup-safe, epsilon-rounded floats)."""
    raise NotImplementedError


def assert_equivalent(spark, baseline: str, candidate: str, *, epsilon: float = 1e-6,
                      partition_cols=None) -> dict:
    """Run the full ladder; RAISE with a localized reason on the first divergence."""
    # counts -> fingerprint -> except_all; never soften epsilon to force a pass.
    raise NotImplementedError
