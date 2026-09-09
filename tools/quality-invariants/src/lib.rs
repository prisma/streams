//! Actual private owners and their tests, without server startup constructors.
//! This allows Miri to reach buffer assertions on Linux: SlateDB's transitive
//! fastant constructor executes unsupported CPUID before the full test binary.
//! No source rewriting, stand-in types, or copied state transitions occur here.
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    reason = "invariant harness; actual crypto types and limits are retained; unrelated service entry points are outside these selected tests"
)]
#[path = "../../../src/crypto.rs"]
mod crypto;
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    reason = "invariant harness; actual postings owner and tests are compiled unchanged; unrelated service entry points are outside this test scope"
)]
#[path = "../../../src/postings.rs"]
mod postings;
#[allow(
    dead_code,
    reason = "invariant harness; actual retained byte owner and tests are compiled unchanged; the library target has no service caller"
)]
#[path = "../../../src/retained_bytes.rs"]
mod retained_bytes;
#[allow(
    dead_code,
    unreachable_pub,
    unused_imports,
    reason = "invariant harness; actual tenant identities preserve crypto dependencies; exporting unused service entries would weaken their boundary"
)]
#[path = "../../../src/tenant.rs"]
mod tenant;

#[allow(
    dead_code,
    reason = "invariant harness; compile actual batch budget and ownership tests together; unused service accessors remain private"
)]
mod application;

#[allow(
    dead_code,
    unreachable_pub,
    reason = "invariant harness; compile the actual queue key codec and generation tests; unrelated service state and commands remain internal"
)]
#[path = "../../../src/queue.rs"]
mod queue;
