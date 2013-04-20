from chll import CubicHyperLogLog

from redis import Redis

class CubicHyperLogLogRedis(CubicHyperLogLog):
	def __init__(self, r, keyname, bits = 9):
		"""
		Construct new HyperLogLog couter
		name	name of storage object, for example MY_COUNTER
		bits	number of bytes, e.g. self.m
		r	redis instance
		"""
		
		super(CubicHyperLogLogRedis, self).__init__(bits)

		self.keyname = keyname
		self.r       = r


	def rkey(self, pos, val):
		return str(pos) + ":" + str(val)


	def add(self, item):
		"""
		Adds the item to the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		self.r.sadd(self.keyname, self.rkey(pos, val))


	def remove(self, item):
		"""
		Removes !!! the item from the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		self.r.srem(self.keyname, self.rkey(pos, val))


	def clear(self):
		"""
		Clears external storage
		"""
		self.r.delete(self.keyname)


	def contains(self, item):
 		"""
 		Bloom filter like functionality,
		checks !!! if the item is already in the HyperLogLog
		x.contains(item)
		"""
		(pos, val) = self.get_pos_val(item)

		#print val, self.MM[pos]

		return self.r.sismember(self.keyname, self.rkey(pos, val))


	def load(self):
		"""
		Loads the HyperLogLog counter from Redis
		"""
		self.reset_MM()

		# Expect up to 512 * 255 "columns"
		dbset = self.r.smembers(self.keyname)
				
		for pos in range(self.m) :
			for val in range(256) :
				k = self.rkey(pos, val)
				if k in dbset :
					self.MM[pos].add(val)
		



