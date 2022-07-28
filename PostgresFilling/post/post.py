from datetime import date


class Post:
    def __init__(self, name: str, author: str, description: str, created_at: date, likes: int):
        self._name = name
        self._author = author
        self._description = description
        self._created_at = created_at
        self._likes = likes

    def __str__(self):
        return (f"name: {self._name}\n"
                f"author: {self._author}\n"
                f"description: {self._description}\n"
                f"created at: {self._created_at}\n"
                f"likes count: {self._likes}")

    def to_tuple(self):
        return self._name, self._author, self._description, self._created_at, self._likes

    @property
    def name(self):
        return self._name

    @property
    def author(self):
        return self._author

    @property
    def description(self):
        return self._description

    @property
    def created_at(self):
        return self._created_at

    @property
    def likes(self):
        return self._likes
