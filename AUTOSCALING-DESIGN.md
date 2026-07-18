# Scaling Groups for Prisma Compute — a design proposal

**From:** Prisma Streams team · 2026-07-15
**Shaped by:** two weeks of fleet pilots on Prisma Compute (runs 3–9, chaos +
soak campaigns, the edge/egress investigations with the platform team), the
EC2 Auto Scaling Group model, and Vercel's Fluid Compute.

---

## The mental model (one sentence)

Every service is a **scaling group** with three numbers — `min`, `max`, and
`desired` — and the platform does two jobs: **keep reality equal to
`desired`** (start, wake, replace, retire instances), and, unless you take
over, **move `desired` for you** to hold per-instance concurrency at a
target.

That's the whole product. Everything below is defaults, guardrails, and the
escape hatches.

Why this shape: EC2 ASGs proved that "desired capacity, adjusted manually or
by policies" is the most durable scaling abstraction in the industry — every
operator already knows it. Fluid proved the *default policy* should be
concurrency ("how many requests is each instance juggling"), not CPU, and
that zero configuration must work. Prisma Compute has one structural
advantage neither has: **millisecond scale-to-zero**, which makes idle
capacity nearly free and lets this design be simpler than both.

---

## 1. The group

```
service: my-app
scaling:
  min: 0          # never fewer available slots than this
  max: 3          # never more than this (the cost ceiling)
  desired: auto   # managed by policy; or a number; or external
```

- **`desired` is "capacity slots," not "running VMs."** The router routes
  across `desired` instances; any of them that are idle sleep (standby
  snapshot) and wake in milliseconds when traffic arrives. This is the
  scale-to-zero-native reinterpretation of the ASG: on EC2, desired
  capacity is expensive to hold, so ASGs bolt on warm pools; here, a
  sleeping slot costs (almost) nothing, so `desired` can be generous.
  Billing follows Fluid's insight: charge for **active CPU + memory-seconds
  while live**, not for slots — then `max: 10` on an idle service costs
  nothing, and raising `max` stops being scary.
- **The platform maintains `desired`**: starts/wakes instance N+1 on
  scale-out (waking is the platform's job — routing must never be the wake
  mechanism; we deadlocked a fleet by assuming otherwise), replaces
  unhealthy instances, spreads across hosts.
- **Health is truthful and self-healing**: instance states are
  `healthy | degraded | crashlooping | stopped`. A crashed instance
  restarts with exponential backoff (the platform already implements this
  as `restart_policy: on-failure`; make it the group default). A
  crash-looping instance keeps retrying on backoff *forever* and shows as
  `crashlooping` — never a silent 404 with a status that says `running`
  (see our repro package; this single change removes the worst operational
  failure we hit).

## 2. Who sets `desired` — three modes, strictly layered

**Mode A — Automatic (the default; zero config).**
Target tracking on **per-instance concurrency**: the platform already
tracks in-flight and queued requests per instance — it should scale on its
own numbers. Default: keep instances at **75 % of their concurrency
budget** (today ≈ 145 concurrent through the front door, so target ≈ 110).
Need is computed proportionally, ASG-style:

```
desired = clamp(ceil(fleet_inflight / (0.75 × budget_per_instance)), min, max)
```

Concurrency is the right default because it is workload-independent (no
per-app calibration), it is the resource the platform actually allocates,
and it moves *before* users feel pain — queue depth is its leading edge.
CPU is the wrong default: we measured real workloads saturating delivery at
16–25 % CPU. CPU remains available as a policy metric for CPU-bound apps.

**Mode B — Manual.** `compute scale my-app --desired 4`. Sets the number,
clamped to [min, max]; automatic policy is suspended until re-enabled.
Exactly ASG semantics; every operator's muscle memory works.

**Mode C — External controller.** A service (or ops system) with a scoped
token writes `desired` via API. This is ASG's `SetDesiredCapacity`, made a
first-class mode. It is also exactly what our Streams fleet does today
("inverted autoscaling": the app computes its own capacity from CPU,
in-flight, memory, commit latency, and router-observed client latency, and
publishes one number). Sophisticated tenants get full control without the
platform having to understand their metrics; the platform still enforces
min/max, health, and pays no attention to *why*.

The three modes are one mechanism: something writes `desired`; the platform
makes it real. There is nothing else to learn.

## 3. Policies, for the 10 % who outgrow the default

- **Target tracking** on one metric: `concurrency` (default), `cpu`,
  `queue_depth`, or `custom` (the app POSTs a gauge — this lets an app
  contribute a signal like "commit latency" without becoming a full Mode-C
  controller).
- **Multiple policies compose by `max()`**: if the CPU policy wants 2 and
  the concurrency policy wants 4, desired is 4. This is ASG's rule, it is
  what our fleet converged on independently (every signal we run exists
  because a specific outage demanded it), and it prevents policy fights.
- Scheduled and predictive scaling are future work; the model accommodates
  them as more writers of the same number.

## 4. Guardrails baked into the platform (not into every app)

These are the failure-earned rules from our pilots, encoded once:

1. **Scale out fast, scale in slow.** Out: immediately, proportionally
   (ceil of need, not +1 loops — wake cost is milliseconds, so there is no
   reason to creep). In: only after the need has been below the target for
   a sustained window (default **5 minutes**), only one step at a time, and
   only if the survivors would sit below a *conservative* threshold
   (default **50 %**, vs the 75 % scale-out target) — the gap is what
   prevents flapping at the boundary.
2. **Never scale in while requests are queueing.** The router sees
   per-instance queue depth; any queueing anywhere in the group blocks
   scale-in outright. We watched a fleet scale *in* during a client-side
   latency collapse because delivered rps fell while clients queued —
   server-side metrics cannot see client pain, but the platform router can,
   natively.
3. **Damp breach-triggered actions ~30 s.** Scale transitions themselves
   spike CPU and latency for a few seconds (shard/connection handoffs); an
   undamped trigger turns every transition into another one.
4. **The router sheds visibly instead of buffering invisibly.** Bounded
   per-instance queue (default: 2 × concurrency budget or 5 s, whichever
   first), then **429 + Retry-After**. Silent multi-second edge buffering
   made overload invisible to every backend metric we had; it cost us a
   full day of misdiagnosis. Shedding also keeps instances healthy enough
   to serve their own heartbeats/health checks — a control plane must
   survive the overload it exists to manage.
5. **Published budgets.** Per-instance ingress concurrency (~145 today),
   per-instance egress (~50 today — this one bounds any service that calls
   other services and must be on the limitations page), instance sizes.
   Autoscaling against unpublished numbers is calibration by archaeology —
   we know, we did it.

## 5. Defaults (the part most users never change)

| setting | default | rationale |
|---|---|---|
| `min` | 0 | scale-to-zero is this platform's superpower; wake ≈ ms |
| `max` | 3 | a real cost ceiling, deliberately raised, cheap while idle |
| policy | target-tracking `concurrency` @ 75 % of budget | zero-config, workload-independent, leads user pain |
| scale-out | immediate, proportional | wake is cheap; under-capacity is not |
| scale-in | 5 min sustained below 50 %, 1 step, blocked while queueing | flap- and collapse-proof |
| health | HTTP check if configured, else listener; `on-failure` restarts w/ backoff | crashes self-heal; crash-loops stay visible, never silent |
| router queue | 2× budget or 5 s, then 429 + Retry-After | overload visible to everyone |
| billing | active CPU + memory-seconds while live | Fluid's alignment: slots are free, work costs |

A new app deploys with **no scaling configuration at all** and gets:
scale-to-zero, wake-on-request, autoscale 0→3 on concurrency, crash
recovery with backoff, and honest status. That is the Fluid bar for
simplicity, on an ASG-shaped foundation everyone already understands.

## 6. What this gives our own use case (the acid test)

Streams is the stress case: stateful shards, self-computed capacity from
five signals, fencing-based ownership. Under this design we: set
`min: 1, max: 64`, use **Mode C** to write `desired` from our fleet
controller, read the *platform's* per-instance in-flight/queue/CPU metrics
instead of instrumenting our own (deleting code), and inherit guardrails
1–4 for free. Everything our pilots had to discover the hard way — wake
ownership, queue-aware scale-in, damping, truthful health — is platform
behavior instead of tenant folklore. A simple stateless app, meanwhile,
configures nothing.

## 7. Explicitly out of scope (kept out to stay understandable)

Mixed instance types and spot-style purchase options (ASG features that
don't map here yet), predictive scaling, zonal balancing, lifecycle hooks.
The model accommodates all of them later as either more policy writers
(§3) or more group maintenance behaviors (§1) without changing the mental
model.

---

*References: [EC2 Auto Scaling groups](https://docs.aws.amazon.com/autoscaling/ec2/userguide/auto-scaling-groups.html)
(min/max/desired, policies adjust desired between bounds, health-check
replacement, multi-policy max semantics); [Vercel Fluid
Compute](https://vercel.com/fluid) (in-function concurrency, active-CPU
pricing, zero-knob defaults); our measurement record in
EXPERIMENT-PILOT.md, PLATFORM-EDGE-REPORT.md (two-layer concurrency
budgets), and repro-no-restart/ (health/restart behavior).*
