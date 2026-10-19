"""Caller-owned MCP deadlines; task-local metadata, never tool arguments or config."""
from contextlib import contextmanager
from contextvars import ContextVar
import math
import time

_deadline = ContextVar('mcp_call_deadline', default=None)


class MCPCallDeadlineExpired(TimeoutError):
    """The caller's budget elapsed, independently of provider health."""


def current_deadline():
    return _deadline.get()


def remaining_timeout(timeout=None, *, deadline=None):
    deadline = current_deadline() if deadline is None else deadline
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise MCPCallDeadlineExpired('MCP caller deadline expired')
    return remaining if timeout is None else min(float(timeout), remaining)


@contextmanager
def call_deadline(deadline):
    if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
        raise ValueError('deadline_monotonic must be a finite number')
    parent = current_deadline()
    effective = deadline if parent is None else parent if deadline is None else min(parent, deadline)
    token = _deadline.set(effective)
    try:
        remaining_timeout()
        yield
        remaining_timeout()
    finally:
        _deadline.reset(token)
