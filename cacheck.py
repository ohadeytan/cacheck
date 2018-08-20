import time
from policies import LRU, WTinyLFU, AdaptiveWTinyLFU, WC_WTinyLFU, WI_WTinyLFU
from parsers import LirsParser

def run(trace, policy):
    for item in trace:
        policy.record(item)
    return policy.get_stats()

def main():
    print("{:<12} {:<12} {:<12} {:<12} {:<12}".format('Name', 'Hits', 'Misses', 'Hit Ratio', 'Time(s)'))
    policies = [LRU(1000), WTinyLFU(1000), WI_WTinyLFU(1000), WC_WTinyLFU(1000)]
    for policy in policies:
        start = time.time()
        trace = LirsParser('sample_trace.tr')
        results = run(trace, policy)
        end = time.time()
        print("{name:<12} {hits:<12} {misses:<12} {hit ratio:<12} {time:<12}".format(**results, time=end - start))

if __name__ == "__main__":
    main()
