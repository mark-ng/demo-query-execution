from heapq import heappop, heappush

def select(it):
    for row in it:
        print(row)

def comparisonMatch(comparison_symbol, target):
    if comparison_symbol == "<":
        return lambda value: value < target
    elif comparison_symbol == ">":
        return lambda value: value > target
    elif comparison_symbol == "<=":
        return lambda value: value <= target
    elif comparison_symbol == ">=":
        return lambda value: value >= target
    elif comparison_symbol == "=":
        return lambda value: value == target
    else:
        raise ValueError("Invalid comparison symbol: {}".format(comparison_symbol))     

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
    
class PredicateIterator:
    def __init__(self, col_idx, predicate, it):
        self.col_idx = col_idx
        self.predicate = predicate
        self.it = it
        self.next_row = None
        self._advance_to_match()

    def _advance_to_match(self):
        for row in self.it:
            if self.predicate(row[self.col_idx]):
                self.next_row = row
                return
            
        self.next_row = None

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.next_row == None:
            raise StopIteration
        row = self.next_row
        self._advance_to_match()
        return row

class NestedLoopJoinIterator:

    def __init__(self, it1, it2, col_idx1, col_idx2):
        self.it1 = it1
        self.it2 = it2
        self.col_idx1 = col_idx1
        self.col_idx2 = col_idx2
        self.next_row = None

        self.complete_full_scan = True
        self.current_row_t1 = None
        self.materialize = []
        self.t2_idx = 0
        self.materialized_mode = False

    def _advance_to_match(self):
        while True:
            if self.complete_full_scan:
                try:
                    self.current_row_t1 = next(self.it1)
                    self.complete_full_scan = False
                except StopIteration:
                    self.next_row = None
                    return

            if not self.materialized_mode:
                try:
                    current_row_t2 = next(self.it2)
                    self.materialize.append(current_row_t2)
                    if current_row_t2[self.col_idx2] == self.current_row_t1[self.col_idx1]:
                        self.next_row = self.current_row_t1 + current_row_t2
                        return
                except StopIteration:
                    self.materialized_mode = True
                    self.complete_full_scan = True
            else:
                if self.t2_idx >= len(self.materialize):
                    self.complete_full_scan = True
                    self.t2_idx = 0
                    continue
                if self.materialize[self.t2_idx][self.col_idx2] == self.current_row_t1[self.col_idx1]:
                    self.next_row = self.current_row_t1 + self.materialize[self.t2_idx]
                    self.t2_idx += 1
                    return
                else:
                    self.t2_idx += 1

    def __iter__(self):
        return self
    
    def __next__(self):
        self._advance_to_match()
        if self.next_row == None:
            raise StopIteration
        row = self.next_row
        return row

movies = [
    # movie_id, title, year
    (1, 'The Matrix', 1999),
    (2, 'Avatar', 2009),
    (3, 'Harry Potter', 2003),
    (4, 'The Matrix', 2000),
    (5, 'Pokemon', 2020)
]

ratings = [
    # movie_id, rating
    (1, 10),
    (2, 9),
    (3, 8),
    (4, 5),
    (5, 6),
    (2, 8),
]

years = [
    # year, name
    (2020, "Two Zero Two Zero"),
    (2000, "Two Zero Zero Zero"),
    (2100, "Two One Zero Zero"),
    (1999, "One Nine Nine Nine")
]

# SELECT * FROM movies;
select(SeqScanIterator(InMemoryIterator(movies)))
print()

# SELECT * FROM movies ORDER BY title;
select(SortIterator(1, SeqScanIterator(InMemoryIterator(movies))))
print()

# SELECT * FROM movies WHERE title = 'The Matrix';
select(PredicateIterator(1, comparisonMatch(">", 'The Matrix'), SeqScanIterator(InMemoryIterator(movies))))
print()

# SELECT * FROM movies WHERE year >= 2003;
select(PredicateIterator(2, comparisonMatch(">=", 2003), SeqScanIterator(InMemoryIterator(movies))))
print()

# SELECT * FROM movies WHERE year >= 2003 ORDER BY year
select(SortIterator(2, PredicateIterator(2, comparisonMatch(">=", 2003), SeqScanIterator(InMemoryIterator(movies)))))
print()

# SELECT * FROM movies JOIN ratings ON movies.movie_id = ratings.movie_id;
select(NestedLoopJoinIterator(SeqScanIterator(InMemoryIterator(movies)), SeqScanIterator(InMemoryIterator(ratings)), 0, 0))
print()

# SELECT * FROM movies 
# JOIN ratings ON movies.movie_id = ratings.movie_id
# JOIN years on movies.year = years.year
select(NestedLoopJoinIterator(NestedLoopJoinIterator(SeqScanIterator(InMemoryIterator(movies)), SeqScanIterator(InMemoryIterator(ratings)), 0, 0), SeqScanIterator(InMemoryIterator(years)), 2, 0))
print()