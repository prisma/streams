---- MODULE Counter ----
\* Driver self-test fixture (scripts/quality/formal.py self-test): a finite
\* counter whose search completes, with an invariant that holds and a
\* witness that is reachable. Not a model of Prisma Streams.
EXTENDS Naturals
VARIABLE x
Init == x = 0
Next == \/ x < 3 /\ x' = x + 1
        \/ x = 3 /\ UNCHANGED x
Spec == Init /\ [][Next]_x
Bounded == x \in 0..3
Witness_ReachesTwo == x # 2
====
