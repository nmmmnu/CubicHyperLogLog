#!/usr/bin/python

from chllredis import CubicHyperLogLogRedis
from redis    import Redis

r = Redis("localhost")

test_cardinalities = [
	1, 2, 5, 10, 20, 50,
	100, 101, 102, 103, 110, 
	1000, 1500, 
	10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 
	#100000,
	#1000000
]

test_cardinalities = [ 100 ]

line = "-" * 62

print line
print "| %5s | %10s | %10s | %10s | %10s  |" % ( "bits", "card", "estim", "diff", "diff" )
print line

for card in test_cardinalities:
	x = CubicHyperLogLogRedis(r, "my_counter", 9)
			
	for i in range(card) :
		x.add(str(i))
	
	x.load()

	card2 = len(x)
	perc = float(card - card2) / card * 100
	
	print "| %5d | %10d | %10d | %10d | %10.2f%% |" % ( x.m, card, card2, card - card2, perc )

print line


