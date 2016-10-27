import numpy as np

num_documents = 700
num_hash_functions = 100
num_bands = 20

def generate_hash_function(p=1507):
    a = np.random.randint(1, np.iinfo(np.int64).max)
    b = np.random.randint(np.iinfo(np.int64).max)

    def hash_function(row_number):
        return ((a * row_number + b) % p) % num_documents

    return hash_function


def hash_row(hash_function, row_numbers):
    min_hash = np.iinfo(np.int64).max
    for row in row_numbers:
        hash = hash_function(row)
        if hash < min_hash:
            min_hash = hash

    return min_hash


hash_function_list_1 = list()
hash_function_list_2 = list()

for i in range(0, num_hash_functions):
    hash_function_list_1.append(generate_hash_function())

for i in range(0, num_bands):
    hash_function_list_2.append(generate_hash_function())

hash_function_list_1 = np.asarray(hash_function_list_1)
hash_function_list_2 = np.asarray(hash_function_list_2)


def mapper(key, value):
    # key: None
    # value: one line of input file
    tokens = value.split()
    doc_id = tokens[0]
    row_numbers = map(int, tokens[1:])
    sig_col = list()

    # generating signature matrix column for this document
    for curr_hash_function in hash_function_list_1:
        sig_col.append(hash_row(curr_hash_function, row_numbers))

    rows_per_band = num_hash_functions / num_bands

    # hash each "strip" in each band for this document
    for band_number in range(0, num_bands):
        current_sum = 0
        current_strip = list()

        # build up this strip in this band
        for r in range(band_number * rows_per_band, (band_number + 1) * rows_per_band):
            current_strip.append(int(sig_col[r]))

        # we now have the current strip
        current_sum += hash_row(hash_function_list_2[band_number], current_strip)
        yield('band' + str(band_number) + ' hash' + str(current_sum), 'doc ' + doc_id)


def reducer(key, values):
    # key: key from mapper used to aggregate
    # values: list of all value for that key
    yield(key, tuple(values))
