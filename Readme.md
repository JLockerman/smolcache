# smolcache #

CLOCK based approximate-LRU cache designed for concurrent usage.
This cache uses a sharded hashmap, and an approximate LRU algorithm
based on CLOCK to minimize the amount of contention amongst multiple
accessors. The CLOCK algorithm means that `Get`s need to perform at
most 1 write, and as the frequency-of-access of an element increases,
the likelyhood a `Get` need to perform a write _decreases_.
