from libc.stdlib cimport malloc, free

cdef class CMS(object):
    cdef unsigned long* SEED
    cdef unsigned long* table 
    cdef unsigned int table_mask, step, additions, period
    def __init__(self, maximum_size, step=1):
        self.SEED = <unsigned long*>malloc(4 * sizeof(unsigned long))
        self.SEED[:] = [0xc3a5c85c97cb3127, 0xb492b66fbe98f273, 0x9ae16a3b2f90404f, 0xcbf29ce484222325]
        next_power_of_two = 2**(len(bin(maximum_size-1))-2)
        self.table = <unsigned long*>malloc(next_power_of_two * sizeof(unsigned long))
        for i in range(next_power_of_two):
            self.table[i] = 0
        self.table_mask = next_power_of_two-1
        self.step = step
        self.additions = 0
        self.period = 10*next_power_of_two

    def increment(self, unsigned long e):
        cdef unsigned int hash_value = self.spread(self.java_long_hash(e))
        cdef unsigned int start = (hash_value & 3) << 2
        cdef unsigned int count
        added = self.increment_at(self.index_of(hash_value, 0), start    ), \
                self.increment_at(self.index_of(hash_value, 1), start + 1), \
                self.increment_at(self.index_of(hash_value, 2), start + 2), \
                self.increment_at(self.index_of(hash_value, 3), start + 3) 
        if any(added):
            self.additions += self.step
            if self.additions >= self.period:
                count = self.reset()
                self.additions = (self.additions >> 1) - (count >> 2)

    def frequancy(self, unsigned long e):
        cdef unsigned int hash_value = self.spread(self.java_long_hash(e))
        cdef unsigned int start = (hash_value & 3) << 2
        indexes = [self.index_of(hash_value, i) for i in range(4)]
        return min([((self.table[indexes[i]] >> ((start + i) << 2)) & 0xf) for i in range(4)])

    cdef unsigned int reset(self):
        cdef unsigned int count = 0
        for i in range(self.table_mask + 1):
            count += bin(self.table[i] & 0x1111111111111111).count('1')
            self.table[i] = (self.table[i] >> 1) & 0x7777777777777777
        return count

    cdef unsigned int index_of(self, unsigned int item, unsigned int i):
        cdef unsigned long hash_value = self.SEED[i] * item
        hash_value = (hash_value + (hash_value >> 32)) 
        return (<unsigned int>hash_value) & self.table_mask

    cdef int increment_at(self, unsigned int i, unsigned int j):
        cdef unsigned int offset = j << 2
        cdef unsigned long mask = 0xf << offset
        cdef unsigned long current, update 
        if (self.table[i] & mask) != mask:
            current = (self.table[i] & mask) >> offset
            update = min(current + self.step, <unsigned long>15)
            self.table[i] = (self.table[i] & ~mask) | (update << offset)
            return 1
        return 0

    def show(self):
        print('------------------')
        for i in range(self.table_mask+1):
            print(hex(self.table[i])[2:].zfill(16))

    cdef unsigned int java_long_hash(self, unsigned long key):
        return (key ^ (key >> 32)) 

    cdef unsigned int spread(self, unsigned int x, int random_seed=1033096058):
        x = (((x >> 16) ^ x) * 0x45d9f3b) 
        x = (((x >> 16) ^ x) * random_seed) 
        return ((x >> 16) ^ x)

    def __dealloc__(self):
        free(self.table)
