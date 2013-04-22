from chll import CubicHyperLogLog

import pycassa

class CubicHyperLogLogCassandra(CubicHyperLogLog):
	def __init__(self, cf, keyname, bits = 9, mutator = None):
		"""
		Construct new HyperLogLog couter
		cf	initialized and "connected" pycassa.ColumnFamily
		keyname	Cassandra row_key MY_COUNTER
		bits	number of bytes, e.g. self.m
		mutator	initialized and "connected" pycassa.batch.Mutator
		
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
		self.mutator = mutator


	def rkey(self, pos, val):
		return str(pos) + ":" + str(val)


	def add(self, item):
		"""
		Adds the item to the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		col = self.rkey(pos, val)
		if col:
			if self.mutator:
				self.mutator.insert(self.cf, self.keyname, { col : "1" } )
			else:
				self.cf.insert(              self.keyname, { col : "1" } )


	def remove(self, item):
		"""
		Removes !!! the item from the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		# Some care, because not to delete whole row
		col = self.rkey(pos, val)
		if col:
			if self.mutator:
				self.mutator.remove(self.cf, self.keyname, [ col ] )
			else:
				self.cf.remove(              self.keyname, [ col ] )


	def clear(self):
		"""
		Clears external storage
		"""
		if self.mutator:
			self.mutator.remove(self.cf, self.keyname)
		else:
			self.cf.remove(              self.keyname)


	def contains(self, item):
 		"""
 		Bloom filter like functionality,
		checks !!! if the item is already in the HyperLogLog
		x.contains(item)
		"""
		(pos, val) = self.get_pos_val(item)

		col = self.rkey(pos, val)
		if col:
			try:
				# We do not check about the mutator here
				self.cf.get(self.keyname, columns=[col])
				return True
			except pycassa.NotFoundException:
				return False
				
		# If we are here, this means there is no col...
		return True
				
		#print val, self.MM[pos]

		return self.r.sismember(self.keyname, self.rkey(pos, val))
		
		
	def load(self):
		"""
		Loads the HyperLogLog counter from Redis
		"""
		self.reset_MM()

		try:
			# Expect up to 512 * 255 "columns"
			dbset = self.cf.get( self.keyname, column_count = self.m * 256 )

			for pos in range(self.m) :
				for val in range(256) :
					k = self.rkey(pos, val)
					if k in dbset :
						self.MM[pos].add(val)
		except pycassa.NotFoundException:
			pass
		


		



