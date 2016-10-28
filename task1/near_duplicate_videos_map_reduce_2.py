import numpy as np


def mapper(key, value):
    r = 60
    b = 20
    n = 700
    p = 23081

    tokens = value.split()
    video_id = int(tokens[0][6:])
    shingles = map(int, tokens[1:])
    sig_mat_col = list()

    np.random.seed(0)

    for i in range(r * b):
        a = np.random.randint(1, np.iinfo(np.int16).max)
        x = np.random.randint(np.iinfo(np.int16).max)
        assert type(a) is int
        assert type(x) is int
        min_hash = np.iinfo(np.int16).max
        for shingle in shingles:
            assert type(shingle) is int
            curr_hash = (a * shingle + x) #((a * shingle + x) % p) % n
            if curr_hash < min_hash:
                min_hash = curr_hash

        sig_mat_col.append(min_hash)

    assert len(sig_mat_col) == r * b, 'len ' + str(len(sig_mat_col)) + ' r * b ' + str(r * b)

    hash_sum = 0
    np.random.seed(1)


    for row_num in range(r * b):
        band_num = row_num / r
        # print 'band number is ' + str(band_num)
        a = np.random.randint(1, np.iinfo(np.int16).max)
        x = np.random.randint(np.iinfo(np.int16).max)

        hash_sum += (a * sig_mat_col[row_num] + x)# ((a * sig_mat_col[row_num] + x) % p) % n

        # if we reach the end of the band, yield a tuple
        if (row_num + 1) % r == 0:
            # print 'end of band ' + str(band_num)
            band_hash = hash_sum % p
            hash_sum = 0
            yield('band_' + str(band_num) + '_hash_' + str(band_hash), video_id)


def reducer(key, values):
    if len(values) > 1:
        values.sort()
        for i in range(len(values)):
            for j in range(i + 1, len(values)):
                yield(values[i], values[j])
