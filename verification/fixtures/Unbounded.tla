---- MODULE Unbounded ----
\* Driver self-test fixture: an infinite state space, so a bounded run can
\* only be incomplete. Not a model of Prisma Streams.
EXTENDS Naturals
VARIABLE x
Init == x = 0
Next == x' = x + 1
Spec == Init /\ [][Next]_x
Typed == x \in Nat
====
