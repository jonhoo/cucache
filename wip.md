Max item limit
  - Use this + available memory to determine #records

Table resize (grow in particular)
  - How can we be cleverer about this to allow resizing without locking
    everything?
  - What's the best way to resize? Here are two ways to do it:
    - Allocate a new table, do insert() for each item.
    - Allocate a new table, copy over all items in same location (using
      `copy()`, which should be faster), but keep original hash
      functions **including mod**. Add (at least one) new hash function
      with new (larger) mod. This will slow down the new table (more
      hash functions to check), but should significantly speed up the
      resize itself.

Hash table size power of two
  - Can use & instead of %

Avoid iterating over empty bins for touchall?

Key tagging (from MemC3):
  - https://www.cs.cmu.edu/~dga/papers/memc3-nsdi2013.pdf
  - With each cval, store one byte of the hash of the key ("tag")
  - On read, first scan tags only (no indirect pointer lookup, cheap compare)
  - Only for matching tags do key check

LRU cache eviction using CLOCK (from MemC3):
  - Keep 1 bit per cval
  - Set to 1 on read
  - On evict, look at ith element:
     - if bit is 0, evict i
       - BE CAREFUL ABOUT CONCURRENT READS!
     - if bit is 1, set to 0, move to i+1

Benchmarks:
  - What is the performance as occupancy increases?
  - Facebook-inspired numbers:
    - http://www.ece.eng.wayne.edu/~sjiang/pubs/papers/atikoglu12-memcached.pdf
    - Use proper read:write ratio (Facebook reports >30:1, sometimes 500:1)
    - Use proper sizes (Facebook reports ~32b keys, ~100s of bytes values)
    - Hit rate 95%
    - 90% of keys occur in 10% of requests: Figure 5
      - 10% of keys in 90% of requests?
    - Key size distribution: GEV dist u = 30.7984, sig = 8.20449, k = 0.078688
      Value size distribution: RP dist θ = 0, sig = 214.476, k = 0.348238
  - Use proper key contention!

UDP protocol needs to be implemented

Should key slice be copied (not aliased) on append/prepend to avoid keeping
body+extra around?
