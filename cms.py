import numpy as np

class CMS(object):
    SEED = [0xc3a5c85c97cb3127, 0xb492b66fbe98f273, 0x9ae16a3b2f90404f, 0xcbf29ce484222325]
    def __init__(self, maximum_size, step=1):
        next_power_of_two = 2**(len(bin(maximum_size-1))-2)
        self.table = np.zeros(next_power_of_two, dtype=np.int64)
        self.table_mask = next_power_of_two-1
        self.step = step
        self.additions = 0
        self.period = 10*next_power_of_two
    def increment(self, e):
        hash_value = spread(java_long_hash(e))
        start = (hash_value & 3) << 2
        added = self.increment_at(self.index_of(hash_value, 0), start    ), \
                self.increment_at(self.index_of(hash_value, 1), start + 1), \
                self.increment_at(self.index_of(hash_value, 2), start + 2), \
                self.increment_at(self.index_of(hash_value, 3), start + 3) 
        if any(added):
            self.additions += self.step
            if self.additions >= self.period:
                count = self.reset()
                self.additions = (self.additions >> 1) - (count >> 2)
    def frequancy(self, e):
        hash_value = spread(java_long_hash(e))
        start = (hash_value & 3) << 2
        indexes = [self.index_of(hash_value, i) for i in range(4)]
        return min([((self.table[indexes[i]] >> np.int64(((start + i) << 2))) & np.int64(0xf)) for i in range(4)])
    def reset(self):
        count = 0
        for i in range(len(self.table)):
            count += np.binary_repr(self.table[i] & np.int64(0x1111111111111111)).count('1')
            self.table[i] = (self.table[i] >> np.int64(1)) & np.int64(0x7777777777777777)
        return count
    def index_of(self, item, i):
        hash_value = (self.SEED[i] * np.asscalar(np.int32(item))) & 0xffffffffffffffff
        hash_value = (hash_value + (hash_value >> 32)) & 0xffffffffffffffff
        return (hash_value & 0xffffffff) & self.table_mask
    def increment_at(self, i, j):
        offset = np.int64(j << 2)
        mask = np.int64(0xf) << offset
        if (self.table[i] & mask) != mask:
            current = (self.table[i] & mask) >> offset
            update = np.int64(min(current + self.step, 15))
            self.table[i] = (self.table[i] & ~mask) | (update << offset)
            return True
        return False
    def show(self):
        print('------------------')
        for x in self.table:
            print(hex(x)[2:].zfill(16))

def java_long_hash(key):
    return (key ^ (key >> 32)) & 0xffffffff

def spread(x, random_seed=1033096058):
    x = (((x >> 16) ^ x) * 0x45d9f3b) & 0xffffffff
    x = (((x >> 16) ^ x) * random_seed) & 0xffffffff
    return (x >> 16) ^ x
