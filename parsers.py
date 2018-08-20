from collections import deque

class Parser(object):
    def __init__(self, file_path):
        self.trace_file = open(file_path, 'r')
        self.items = deque()
    def __iter__(self):
        return self
    def __next__(self):
        if not self.items:
            line = self.trace_file.readline()
            if not line:
                self.trace_file.close()
                raise StopIteration()
            self.parse(line)
        return self.items.pop()
    def parse(self, line):
        pass

class ArcParser(Parser):
    def parse(self, line):
        line = line.split()
        start_block = int(line[0])
        self.items.extend(range(start_block, start_block + int(line[1])))

class LirsParser(Parser):
    def parse(self, line):
        if line and line[0] != '*':
            self.items.append(int(line.strip()))

