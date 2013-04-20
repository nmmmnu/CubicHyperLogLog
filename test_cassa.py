#!/usr/bin/python

#
# PyCassa test
#

from chllcassa import CubicHyperLogLogCassandra

from pycassa.pool         import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.batch        import Mutator

pool = ConnectionPool('test', ['localhost:9160'])
cf   = ColumnFamily(pool, 'hll')
mut  = Mutator(pool, 5000)

test_cardinalities = [ 1, 2, 3, 4, 5 ]
test_cardinalities_multiplier = 1000

line = "-" * 62

print line
print "| %5s | %10s | %10s | %10s | %10s  |" % ( "bits", "card", "estim", "diff", "diff" )
print line

for card in test_cardinalities:
	x = CubicHyperLogLogCassandra(cf, "my_counter_test", 9, mutator = mut)
	
	x.clear()
			
	for i in range(card) :
		print i
		for j in range(test_cardinalities_multiplier) :
			x.add(str(i) + "-" + str(j))
	
	mut.send()
	
	x.load()
	
	card  = card * test_cardinalities_multiplier
	card2 = len(x)
	perc = float(card - card2) / card * 100
	
	print "| %5d | %10d | %10d | %10d | %10.2f%% |" % ( x.m, card, card2, card - card2, perc )

	print "Bloomfilter test", ( "Niki" in x ), ( "Peter Peterson" in x ), ( "123-123" in x )
	


