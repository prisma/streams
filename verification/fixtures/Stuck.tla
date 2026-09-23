---- MODULE Stuck ----
\* Driver self-test fixture: a protocol that stops with no enabled action
\* and no declared quiescence, which TLC must report as a deadlock.
EXTENDS Naturals
VARIABLE x
Init == x = 0
Next == x < 2 /\ x' = x + 1
Spec == Init /\ [][Next]_x
Typed == x \in 0..2
====
