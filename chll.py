
import math
from hashlib import sha1
from bisect  import bisect_right

"""
Largely based on:
	https://github.com/svpcom/hyperloglog
	https://github.com/koron/redis-hyperloglog
"""


class CubicHyperLogLog(object):
	BITCOUNT  = None
	
	#sha-1 size is 160 bits, see get_pos_val()
	HASH_SIZE = 160
	
	def hash(self, item):
		return sha1(item)
	
	def __init__(self, bits = 9): 
		"""
		Construct new HyperLogLog couter
		bits		number of bytes, e.g. self.m
		"""
				
		"""
		#Instead of bits, we can supply error_rate.
		#This is how error_rate is used to calculate the bits:
		
		error_rate = 0.05
		bits = int(math.ceil(math.log((1.04 / error_rate) ** 2, 2)))
		"""
		
		if not (4 <= bits <= 16):
			raise ValueError("bits should be in range [4 : 16]")
		
		self.bits  = bits

		self.alpha = self._alpha()
		
		self.m     = 1 << bits
		
		self.reset_MM()
		
		# Initialize static list
		if CubicHyperLogLog.BITCOUNT is None:
			CubicHyperLogLog.BITCOUNT = []
			for i in range(self.HASH_SIZE - bits + 1) :
				CubicHyperLogLog.BITCOUNT.append(1L << i)


	def reset_MM(self):
		self.MM    = []
		for pos in range(self.m) :
			self.MM.append( set() )

		"""
		MM is list of sets:
		[
			0: (0..255)
			1: (0..255)
			2: (0..255)
			...
			m: (0..255)
		]
		
		  1M = 5800 columns
		 10M = 7461 columns
		100M = 9138 columns
		"""
		
		pass

	
	def _alpha(self):
		if self.bits == 4:
			return 0.673

		if self.bits == 5:
			return 0.697

		if self.bits == 6:
			return 0.709

		return 0.7213 / (1.0 + 1.079 / (1 << self.bits))


	def _rho(self, a):
		rho = len(CubicHyperLogLog.BITCOUNT) - bisect_right(CubicHyperLogLog.BITCOUNT, a)
		
		if rho == 0:
			raise ValueError('rho overflow')
			
		return rho

	
	def get_pos_val(self, item):
		"""
		Returns possition and value of the classical byte-structure of the HyperLogLog
		"""
		# SHA1 as long int
		x  = long(self.hash(item).hexdigest(), 16)
		ff = (1 << self.bits) - 1

		pos = x & ff
		val = self._rho(x >> self.bits) 

		return pos, val


	def add(self, item):
		"""
		Adds the item to the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		self.MM[pos].add( val )


	def remove(self, item):
		"""
		Removes !!! the item from the HyperLogLog
		"""
		(pos, val) = self.get_pos_val(item)
		
		try:
			self.MM[pos].remove( val )
		except KeyError:
			# Looks like this was never in counter...
			pass

       
	def update(self, other):
		"""
		Alias of merge(),
		provided for compatibility with hyperloglog package
		"""
		return self.merge(other)


	def merge(self, other):
		"""
		Merges this HyperLogLog counter with other HyperLogLog counter
		"""
		if self.m != other.m:
			raise ValueError('precisions must be equal')
		
		for pos in range(self.m) :
			self.MM[pos].update( other.MM[pos] )
		

	def M(self):
		"""
		Returns classical byte-structure of the HyperLogLog
		"""
		M = []
		for val in range(self.m) :
			M.append(0)
		
		for pos in range(self.m) :
			if self.MM[pos] :
				M[pos] = max( self.MM[pos] )
		
		return M
       
       
	def len(self):
		"""
		Returns the estimate of the cardinality as x.len()
		"""
		
		M = self.M()
		
		s = 0.0
		for x in M:
			s += math.pow(2.0, -x)

		
		E = self.alpha * self.m * self.m / s

		if E <= 2.5 * self.m:
			# Small range correction
			
			# count number or registers equal to 0
			V = M.count(0)
			
			if V > 0:
				return self.m * math.log(self.m / float(V))
			
			# No correction need
			return E
	
		if E <= float(1L << self.HASH_SIZE) / 30.0:
			# Intermidiate range correction, e.g. No correction
			return E

		# Large range correction
		return -(1L << self.HASH_SIZE) * math.log(1.0 - E / (1L << self.HASH_SIZE))
            
	def __len__(self):
		"""
		Returns the estimate of the cardinality as len(x)
		"""
		return int(self.len())


if __name__ == '__main__':
	pass

