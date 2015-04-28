Max item limit
  - Use this + available memory to determine #records

Table resize (grow in particular)
  - How can we be cleverer about this to allow resizing without locking
    all concurrent inserts?
  - What's the best way to resize?
    Currently we just copy over the elements, but we could also allocate
    a new table, copy over all items in same location (using `copy()`,
    which should be faster), but keep original hash functions
    **including mod**. We then add (at least one) new hash function with
    new (larger) mod. This will slow down the new table (more hash
    functions to check), but should significantly speed up the resize
    itself.
  - How should shrink be supported (if at all)?

Avoid iterating over empty bins for touchall?

How should resizing table be traded off against evicting items?
  - Currently a goroutine periodically checks how many items were
    evicted during the last pass, and will resize if this exceeds a
    threshold. This threshold needs to be tweaked.

Benchmarks:
  - What is the performance as occupancy increases?
  - Is 8 the right number of values for each bin? Will slow down lookup
    (even with MemC3 tags)
  - Facebook-inspired numbers:
    - http://www.ece.eng.wayne.edu/~sjiang/pubs/papers/atikoglu12-memcached.pdf
    - Use proper read:write ratio (Facebook reports >30:1, sometimes 500:1)
    - Use proper sizes (Facebook reports ~32b keys, ~100s of bytes values)
    - Hit rate 95%
    - 90% of keys occur in 10% of requests: Figure 5
      - 10% of keys in 90% of requests?
    - Key size distribution: GEV dist u = 30.7984, sig = 8.20449, k = 0.078688
      Value size distribution: RP dist θ = 0, sig = 214.476, k = 0.348238

UDP protocol needs to be implemented

Should key slice be copied (not aliased) on append/prepend to avoid keeping
body+extra around?
