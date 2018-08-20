import time
from policies import LRU, WTinyLFU, AdaptiveWTinyLFU, WC_WTinyLFU, WI_WTinyLFU
from parsers import LirsParser

def run(trace, policy):
    for item in trace:
        policy.record(item)
    return policy.get_stats()

def main():
    print("{:<12} {:<12} {:<12} {:<12}".format('Name', 'Hits', 'Misses', 'Hit Ratio'))
    policies = [LRU(1000), WTinyLFU(1000), WI_WTinyLFU(1000), WC_WTinyLFU(1000)]
    for policy in policies:
        trace = LirsParser('sample_trace.tr')
        results = run(trace, policy)
        print("{name:<12} {hits:<12} {misses:<12} {hit ratio:<12}".format(**results))

if __name__ == "__main__":
    main()
