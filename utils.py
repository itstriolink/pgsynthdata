import random
import string

import radar


def random_word(length):
    if length == 0:
        length += 1

    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def random_date(start_date, end_date):
    return radar.random_date(start=start_date, stop=end_date)


def random_number(start, end):
    return random.randint(start, end)
