from enum import Enum, auto
from cms import CMS
from collections import Counter
from math import log
from scipy import stats

class Policy(object):
    def __init__(self, maximum_size):
        self.maximum_size = maximum_size
        self.misses = 0
        self.hits = 0
        pass
    def record(self, key):
        pass
    def get_stats(self):
        return { 'name' : self.__class__.__name__, 'hits' : self.hits, 'misses' : self.misses, 'hit ratio' : self.hits / (self.hits + self.misses) } 

class LRU(Policy):
    def __init__(self, maximum_size):
        super().__init__(maximum_size)

        self.data = {}
        self.sentinel = Node()

    def record(self, key):
        node = self.data.get(key)
        if node:
            self.hits += 1
            node.remove()
            node.append_to_tail(self.sentinel)
        else:
            self.misses += 1
            if len(self.data) == self.maximum_size:
                del self.data[self.sentinel.next_node.data]
                self.sentinel.next_node.remove()
            new_node = Node(key)
            new_node.append_to_tail(self.sentinel)
            self.data[key] = new_node

class WTinyLFU(Policy):
    def __init__(self, maximum_size, window_percentage=1):
        super().__init__(maximum_size)

        self.data = {}

        self.cms = CMS(maximum_size)

        self.sentinel_window = Node()    # LRU
        self.sentinel_probation = Node() # SLRU
        self.sentinel_protected = Node() # SLRU
        
        self.max_window_size = (self.maximum_size * window_percentage) // 100
        max_main = self.maximum_size - self.max_window_size
        self.max_protected = max_main * 4 // 5

        self.size_window = 0
        self.size_protected = 0

    def record(self, key):
        self.cms.increment(key)
        node = self.data.get(key)
        if not node:
            self.misses += 1
            new_node = Node(key, Node.Status.Window)
            new_node.append_to_tail(self.sentinel_window)
            self.data[key] = new_node
            self.size_window += 1
            if self.size_window > self.max_window_size:
                self.evict()
            return False
        else:
            self.hits += 1
            node.remove()
            if node.status == Node.Status.Window:
                node.append_to_tail(self.sentinel_window)
            elif node.status == Node.Status.Probation:
                node.status = Node.Status.Protected
                node.append_to_tail(self.sentinel_protected)
                self.size_protected += 1
                self.demote_protected()
            elif node.status == Node.Status.Protected:
                node.append_to_tail(self.sentinel_protected)
            return True

    def demote_protected(self):
        if self.size_protected > self.max_protected:
            demote = self.sentinel_protected.next_node
            demote.remove()
            demote.status = Node.Status.Probation
            demote.append_to_tail(self.sentinel_probation)
            self.size_protected -= 1

    def evict(self):
        candidate = self.sentinel_window.next_node
        candidate.remove()
        self.size_window -= 1
        candidate.status = Node.Status.Probation
        candidate.append_to_tail(self.sentinel_probation)
        if len(self.data) > self.maximum_size:
            victim = self.sentinel_probation.next_node
            evicted = victim if self.cms.frequancy(candidate.data) > self.cms.frequancy(victim.data) else candidate
            del self.data[evicted.data]
            evicted.remove()
    
class AdaptiveWTinyLFU(WTinyLFU):
    def adjust(self, wanted_window):
        if len(self.data) < self.maximum_size:
            return
        if wanted_window > self.max_window_size:
            self.increase_window(wanted_window - self.max_window_size)
        elif wanted_window < self.max_window_size:
            self.decrease_window(self.max_window_size - wanted_window)
    def increase_window(self, amount):
        steps = min(amount, self.max_protected)
        for _ in range(steps):
            self.max_window_size += 1
            self.size_window += 1
            self.max_protected -= 1
            self.demote_protected()
            candidate = self.sentinel_probation.next_node
            candidate.remove()
            candidate.status = Node.Status.Window
            candidate.append_to_tail(self.sentinel_window)
    def decrease_window(self, amount):
        steps = min(amount, self.max_window_size)
        for _ in range(steps):
            assert amount > 0
            self.max_window_size -= 1
            self.size_window -= 1
            self.max_protected += 1
            candidate = self.sentinel_window.next_node
            candidate.remove()
            candidate.status = Node.Status.Probation
            candidate.append_to_head(self.sentinel_probation)

# Hill Climbing aprroach
class WC_WTinyLFU(AdaptiveWTinyLFU):
    def __init__(self, maximum_size, window_percentage=1, sample_multiplier=10, pivot=0.05):
        super().__init__(maximum_size, window_percentage)
        self.hits_in_sample = 0
        self.hits_in_prev = 0
        self.sample = 0
        self.sample_size = sample_multiplier * self.maximum_size
        self.pivot = int(pivot * self.maximum_size)
        self.increase_direction = False
    def record(self, key):
        hit = super().record(key)
        if len(self.data) >= self.maximum_size:
            self.climb(hit)
    def climb(self, hit):
        if hit:
            self.hits_in_sample += 1
        self.sample += 1
        if self.sample >= self.sample_size:
            if self.hits_in_prev > 0:
                if (self.hits_in_prev + self.sample * 0.01) > self.hits_in_sample:
                    self.increase_direction = not self.increase_direction
                if self.increase_direction:
                    self.increase_window(self.pivot)
                else:
                    self.decrease_window(self.pivot)
            self.hits_in_prev = self.hits_in_sample
            self.hits_in_sample = 0
            self.sample = 0

# Indicator approach
class WI_WTinyLFU(AdaptiveWTinyLFU):
    def __init__(self, maximum_size, window_percentage=1):
        super().__init__(maximum_size, window_percentage)
        self.sample = 0
        self.sample_size = 50000
        self.indicator = Indicator()
    def record(self, key):
        super().record(key)
        if len(self.data) >= self.maximum_size:
            self.climb(key)
    def climb(self, key):
        self.indicator.record(key)
        self.sample += 1
        if self.sample >= self.sample_size:
            ind = self.indicator.get_indicator()*80.0/100.0
            self.adjust(int(ind*self.maximum_size))
            self.indicator.reset()
            self.sample = 0

class Indicator(object):
    def __init__(self):
        self.cms = CMS(5000)
        self.hinter_sum = 0
        self.hinter_count = 0
        self.freqs = Counter() # Alternatively use SpaceSaving 
    def record(self, key):
        hint = self.cms.frequancy(key)
        self.hinter_sum += hint
        self.hinter_count += 1
        self.cms.increment(key)
        self.freqs[key] += 1
    def get_hint(self):
        return self.hinter_sum / self.hinter_count
    def est_skew(self):
        top_k = [ (i, log(k[1])) for i, k in zip(range(1,71), self.freqs.most_common(70)) ]
        return -stats.linregress(top_k)[0]
    def get_indicator(self):
        skew = self.est_skew()
        return (self.get_hint() * ((1 - skew**3) if skew < 1 else 0)) / 15.0
    def reset(self):
        self.hinter_sum = 0
        self.hinter_count = 0
        self.freqs.clear()

class Node(object):
    def __init__(self, data=None, status=None):
        self.data = data
        self.next_node = self
        self.prev_node = self
        self.status = status
    def remove(self):
        self.prev_node.next_node = self.next_node
        self.next_node.prev_node = self.prev_node
    def append_to_tail(self, sentinel):
        self.prev_node = sentinel.prev_node
        self.next_node = sentinel
        self.prev_node.next_node = self
        self.next_node.prev_node = self
    def append_to_head(self, sentinel):
        self.next_node = sentinel.next_node
        self.prev_node = sentinel
        self.prev_node.next_node = self
        self.next_node.prev_node = self
    class Status(Enum):
        Window    = auto()
        Probation = auto()
        Protected = auto()
