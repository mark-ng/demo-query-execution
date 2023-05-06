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
    
movies = [
    # movie_id, title, year
    (1, 'The Matrix', 1999),
    (2, 'Avatar', 2009),
    (3, 'Harry Potter', 2003),
]

movies_iterator = InMemoryIterator(movies)

for movie in movies:
    print(movie)