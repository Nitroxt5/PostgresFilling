from faker import Faker

from PostgresFilling.post.post import Post


def generate_post():
    fake = Faker()
    # fake.seed(1)
    name = fake.word().title()
    author = fake.name()
    description = fake.sentence(nb_words=10)
    created_at = fake.date_between()
    likes_count = fake.pyint()
    return Post(name, author, description, created_at, likes_count)
