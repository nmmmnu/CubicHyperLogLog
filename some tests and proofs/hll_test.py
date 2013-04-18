import math
from hashlib import sha1

class HLL:
	def __init__(self, name, bits = 9):
		"""
		Construct new HyperLogLog couter
		name =	name of storage object, for example MY_COUNTER
		bits =	4, 5, 6, 7, 8, 9 etc. 
			default is 9 bits, M struct is 512 bytes
		"""
		
		self.M = {}
		
		self.name = name
		
		if not (4 <= bits <= 16):
			raise ValueError("bits should be in range [4 : 16]")
		
		self.bits = bits

	def _hash(self, item):
		"""
		Calculates 32 bit integer hash
		"""
		
		# SHA1 as long int
		x  = long(sha1(item).hexdigest(), 16)
		
		ff = (1 << self.bits) 
		#ff = 0xffffffff
		
		return x % ff

	def _rho(self, n):
		bits_in_hash = 32 - self.bits
		
		if n <= 0 :
			return bits_in_hash + 1

		return bits_in_hash - math.floor(math.log(n) / math.log(2))
	
	def add(self, item):
		"""
		Adds the item to the HyperLogLog couter
		"""
		h = self._hash(item)
		m = 1 << self.bits
		
		index = int(h % m)
		
		# convert to float, so div not to be spoiled
		m = m * 1.0
		
		value = self._rho(h / m )
		
		self.M[index] = max(self.M.get(index, 0), int(value))

       
	def len(self):
		m = 1 << self.bits
		
		alpha = 0
		if   m == 16 :
			alpha = 0.673
		elif m == 32 :
			alpha = 0.697
		elif m == 64 :
			alpha = 0.709
		else:
			alpha = 0.7213 / (1 + 1.079 / m)
			
		sum = 0
		for key in self.M.keys():
			if self.M.get(key) > 0:
				sum += 2 ** - self.M.get(key)
		
		print self.M
		
		len1 = len(self.M)
		len1 = len1 * 1.0
		
		print len1
		
		estimate = alpha * m * m / (sum + m - len1)
		
		print estimate

		if estimate <= 2.5 * m :
			if len1 == m :
				return round(estimate)
			else :
				return round(m * math.log(m / (m - len1)))
		
		if estimate <= (1 << 32) / 30.0 :
			print "here"
			return round(estimate)

		return round(math.pow(-2, 32) * math.log(1 - estimate / math.pow(2, 32) ) )

	def __len__(self):
		return int(self.len())



for card in (1, 2, 5, 10, 100, 1000, 1500, 10000, 100000) :
	x = HLL("my counter")
	
	for i in range(card) :
		x.add(str(i))

	#print x.M
	print "%10d : %10d" % ( card, len(x) )



