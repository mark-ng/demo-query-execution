from heapq import heappop, heappush

def Select(it):
    for row in it:
        print(row)

class InMemoryIterator:
    def __init__(self, rows):
        self.rows = rows
        self.pos = 0

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.pos >= len(self.rows):
            raise StopIteration
        row = self.rows[self.pos]
        self.pos += 1
        return row
    
class SeqScanIterator:
    def __init__(self, it):
        self.it = it

    def __iter__(self):
        return self
    
    def __next__(self):
        return next(self.it)
    
class SortIterator:
    # TODO: Support multi-column sort
    # TODO: support descending order
    def __init__(self, col_idx, it):
        self.col_idx = col_idx
        self.it = it
        self.heap = []
    
        # Sorting on init
        for row in it:
            heappush(self.heap, (row[self.col_idx], row))
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if len(self.heap) == 0:
            raise StopIteration
        return heappop(self.heap)[1]

movies = [
    # movie_id, title, year
    (1, 'The Matrix', 1999),
    (2, 'Avatar', 2009),
    (3, 'Harry Potter', 2003),
]

# SELECT * FROM movies;
Select(SeqScanIterator(InMemoryIterator(movies)))

print()

# SELECT * FROM movies ORDER BY title;
Select(SortIterator(1, SeqScanIterator(InMemoryIterator(movies))))

print()