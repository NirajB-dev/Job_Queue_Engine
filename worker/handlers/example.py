import time
from typing import Any

from core.models import Job
from core.registry import registry


@registry.register("example.echo")
def echo(job: Job) -> dict[str, Any]:
    return {"echo": job.payload}


@registry.register("example.sleep")
def sleep(job: Job) -> dict[str, Any]:
    duration = float(job.payload.get("seconds", 1))
    time.sleep(duration)
    return {"slept": duration}


@registry.register("example.fail")
def fail(job: Job) -> dict[str, Any]:
    raise RuntimeError(job.payload.get("message", "intentional failure"))
