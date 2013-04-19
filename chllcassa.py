from chll import CubicHyperLogLog

import pycassa

class CubicHyperLogLogCassandra(CubicHyperLogLog):
	def __init__(self, cf, keyname, bits = 9):
		"""
		Construct new HyperLogLog couter
		cf	initialized and "connected" PyCassa ColumnFamily
		keyname	Cassandra row_key MY_COUNTER
		bits	number of bytes, e.g. self.m
		
		------------------------------------------
		
		The class utilize Column Family such:
		
		create column family hll
		with 
			comparator = 'AsciiType'		and 
			key_validation_class = 'AsciiType'	and
			default_validation_class= 'UTF8Type';
		"""
		
		super(CubicHyperLogLogCassandra, self).__init__(bits)

		self.keyname = keyname
		self.cf      = cf


	def rkey(self, pos, val):
		return str(pos) + ":" + str(val)


	def add(self, item):
		"""
		Adds the item to the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		col = self.rkey(pos, val)
		if col:
			self.cf.insert(self.keyname, { col : "1" } )


	def remove(self, item):
		"""
		Removes !!! the item from the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		# Some care, because not to delete whole row
		col = self.rkey(pos, val)
		if col:
			self.cf.remove(self.keyname, [ col ])


	def clear(self):
		self.cf.remove(self.keyname)
		
		
	def load(self):
		"""
		Loads the HyperLogLog counter from Redis
		"""
		# Expect up to 512 * 255 "columns"

		dbset = self.cf.get( self.keyname, column_count = self.m * 256 )

		self.reset_MM()
		
		for pos in range(self.m) :
			for val in range(256) :
				k = self.rkey(pos, val)
				if k in dbset :
					self.MM[pos].add(val)
		



