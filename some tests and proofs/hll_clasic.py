import math
from hashlib import sha1
from bisect  import bisect_right

"""
Largely based on:
	https://github.com/svpcom/hyperloglog
	https://github.com/koron/redis-hyperloglog
"""

"""
  1M = 5800 columns
 10M = 7461 columns
100M = 9138 columns
"""

class HLL:
	stats_collector = True
	stats = set()
	
	def __init__(self, name, error_rate = 0.05):
		"""
		Construct new HyperLogLog couter
		name       =	name of storage object, for example MY_COUNTER
		error_rate =	0.05, 0.01 etc. 
		"""
		
		bits = int(math.ceil(math.log((1.04 / error_rate) ** 2, 2)))
		
		self.alpha = self._alpha(bits)
		
		self.bits  = bits
		
		self.m     = 1 << bits
		
		self.M     = []
		for i in range(self.m) :
			self.M.append(0)
			
		self.bitcount_arr = []
		for i in range(160 - bits + 1) :
			self.bitcount_arr.append(1L << i)


	@staticmethod
	def _alpha(b):
		if not (4 <= b <= 16):
			raise ValueError("bits should be in range [4 : 16]" % b)

		if b == 4:
			return 0.673

		if b == 5:
			return 0.697

		if b == 6:
			return 0.709

		return 0.7213 / (1.0 + 1.079 / (1 << b))


	def _rho(self, a):
		rho = len(self.bitcount_arr) - bisect_right(self.bitcount_arr, a)
		
		if rho == 0:
			raise ValueError('rho overflow')
			
		return rho

	
	def get_add(self, item):
		# SHA1 as long int
		x  = long(sha1(item).hexdigest(), 16)
		ff = (1 << self.bits) - 1

		pos = x & ff
		val = self._rho(x >> self.bits) 

		#self.M[pos] = max(self.M[pos], val)
		return pos, val


	def add(self, item):
		(pos, val) = self.get_add(item)
		
		if self.stats_collector:
			self.stats.add( (pos, val) )
		
		self.M[pos] = max(self.M[pos], val)
       
       
	def len(self):
		"""
		Returns the estimate of the cardinality as x.len()
		"""
		
		s = 0.0
		for x in self.M:
			s += math.pow(2.0, -x)

		
		E = self.alpha * self.m * self.m / s

		if E <= 2.5 * self.m:
			# Small range correction
			
			# count number or registers equal to 0
			V = self.M.count(0)
			
			if V > 0:
				return self.m * math.log(self.m / float(V))
			
			# No correction need
			return E
	
		if E <= float(1L << 160) / 30.0:
			# Intermidiate range correction, e.g. No correction
			return E

		# Large range correction
		return -(1L << 160) * math.log(1.0 - E / (1L << 160))
            
	def __len__(self):
		"""
		Returns the estimate of the cardinality as len(x)
		"""
		return int(self.len())



for card in (
	1, 2, 5, 10, 
	100, 101, 102, 103, 110, 
	1000, 1500, 10000, 20000, 
						#   100000,
						#  1000000,
						# 100000000,
							) :
	x = HLL("my counter", 0.05)
	#print x.m, x.bits
	
	for i in range(card) :
		x.add(str(i))

	#print x.M
	print len(x.stats)
	
	l = len(x)
	perc = float(card - l) / card * 100
	
	print "%10d : %10d : %10.2f%%" % ( card, len(x), perc )



