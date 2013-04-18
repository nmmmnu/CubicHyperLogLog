CubicHyperLogLog
================

CubicHyperLogLog is HyperLogLog probabilistic counter.

Unlike clasical HyperLogLog counter, this one is optimized for avoiding "read before write" when adding items.
Unlike clasical HyperLogLog counter, CubicHyperLogLog also allows removing !!! items.

There are (will be soon) following implementations:

1. Redis - avoiding read, gives atomicity.

2. Cassandra - reading, gives great performance and atomicity.
   You will NOT neet to read / write at QUORUM.



How it works:

Classical HyperLogLog uses array (Python list) of several bytes, usually 512 bytes.
Each byte could be 0 to 255.

When we need to add an item, program calculate a hash value, and from this value, 
program decides the array possition (or array index) and the value.
However the value is stored only if it is bigger the value already stored into the array.

This is where the optimization come - instead of single dimention array,
we use two dimentional array.

"X coordinate" of the array is old array.
"Y coordinate" of the array is the value, 0..255.

For best performance, in Python we use list of sets , e.g. [ set(), set(), ]

Notice when we add an item, we set only one array "cell". We can easily send this operation to Redis or Cassandra.

Notice also that with this structure we can also removing an element, by setting the array "cell" to zero.
This is not true in 100% of the cases, but we tried with 100M items and was fairly OK.



Trades, e.g. When you gain something, you lose something:

Getting the probabalistic count will need to read whole two dimentional array.
This could be 512 * 256 = 131 072 (128 KB) columns/keys.
This is of cource when structure is fully "saturated". 
We did some real live tests and results are:

  1M items = 5800 columns needs to be read
 10M items = 7461 columns needs to be read
100M items = 9138 columns needs to be read
      
See cassandra_schema_notes.txt for more information.



