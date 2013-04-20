#!/usr/bin/python

#
# Plain test
#

from chllredis import CubicHyperLogLog

test_cardinalities = [
	1, 2, 5, 10, 20, 50,
	100, 1000, 10000, 100000, 
	1700,
]

line = "-" * 62

print line
print "| %5s | %10s | %10s | %10s | %10s  |" % ( "bits", "card", "estim", "diff", "diff" )
print line

for card in test_cardinalities:
	x = CubicHyperLogLog(9)
	
	# No need to call clear()
	# x.clear()
	
	for i in range(card) :
		x.add(str(i))
	
	x.load()

	card2 = len(x)
	perc = float(card - card2) / card * 100
	
	print "| %5d | %10d | %10d | %10d | %10.2f%% |" % ( x.m, card, card2, card - card2, perc )
	
	#print "Bloomfilter test", ( "Niki" in x ), ( "Peter Peterson" in x ), ( str(123) in x )

print line


