import numpy as np


def mapper(key, value):
    r = 20
    b = 100
    n = 8192
    p = 8209

    tokens = value.split()
    video_id = int(tokens[0][6:])
    shingles = map(int, tokens[1:])
    sig_mat_col = list()

    np.random.seed(0)

    for i in range(r * b):
        a = np.random.randint(1, n)
        x = np.random.randint(n)
        assert type(a) is int
        assert type(x) is int
        min_hash = np.iinfo(np.int16).max
        for shingle in shingles:
            assert type(shingle) is int
            curr_hash = ((a * shingle + x) % p) % n
            if curr_hash < min_hash:
                min_hash = curr_hash

        sig_mat_col.append(min_hash)

    assert len(sig_mat_col) == r * b, 'len ' + str(len(sig_mat_col)) + ' r * b ' + str(r * b)

    hash_sum = 0
    np.random.seed(1)

    for row_num in range(r * b):
        band_num = row_num / r
        a = np.random.randint(1, n)
        x = np.random.randint(n)

        hash_sum += ((a * sig_mat_col[row_num] + x) % p) % n

        # if we reach the end of the band, yield a tuple
        if (row_num + 1) % r == 0:
            # print 'end of band ' + str(band_num)
            band_hash = hash_sum % n
            hash_sum = 0
            yield('band_' + str(band_num) + '_hash_' + str(band_hash), video_id)


def reducer(key, values):
    if len(values) > 1:
        with open('data/handout_shingles.txt') as f:
            shingles = f.readlines()
            values.sort()
            for i in range(len(values)):
                for j in range(i + 1, len(values)):
                    # compare the candidate pairs
                    shingles1 = set(map(int, shingles[values[i]].split()[1:]))
                    shingles2 = set(map(int, shingles[values[j]].split()[1:]))

                    if float(len(shingles1.intersection(shingles2))) / float(len(shingles1.union(shingles2))) > 0.85:
                        yield(values[i], values[j])
