import random
import string

import radar


def random_word(length, numeric=False):
    if length == 0:
        length += 1
    letters = string.ascii_lowercase
    numbers = string.digits

    if numeric:
        return ''.join(random.choice(numbers) for i in range(length))
    else:
        return ''.join(random.choice(letters) for i in range(length))


def random_date(start_date, end_date, time=False):
    if time:
        return radar.random_datetime(start=start_date, stop=end_date)
    else:

        return radar.random_date(start=start_date, stop=end_date)


def random_number(start, end, uniform=False):
    start = int(start)
    end = int(end)

    if uniform:
        return random.uniform(start, end)
    else:
        return random.randint(start, end)


def random_boolean(postgres=True):
    random_boolean = random.choice([True, False])
    if not postgres:
        return random_boolean
    else:
        if random_boolean:
            return 'true'
        else:
            return 'false'


def random_choice(from_list):
    return random.choice(from_list)


def random_choices(list, weights, k=1):
    if k == 1:
        return random.choices(list, weights=weights, k=k)[0]
    else:
        return random.choices(list, weights=weights, k=k)
