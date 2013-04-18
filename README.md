CubicHyperLogLog
================

CubicHyperLogLog is HyperLogLog probabilistic counter.

Unlike clasical HyperLogLog counter, this one is optimized for avoiding "read before write" when adding items.

There are (will be soon) following implementations:

1. Redis - avoiding read, gives atomicity.

2. Cassandra - reading, gives great performance and atomicity.
   You will NOT neet to read / write at QUORUM.
