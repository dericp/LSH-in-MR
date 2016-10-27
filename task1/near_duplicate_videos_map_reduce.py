import numpy as np

r = 300
b = 20
num_documents = 700
num_hash_functions = b * r
num_sig_matrix_rows = num_hash_functions
num_bands = b
# 50 rows in each band

def generate_hash_function(p=23081):
    a = np.random.randint(1, np.iinfo(np.int64).max)
    b = np.random.randint(np.iinfo(np.int64).max)

    def hash_function(row_number):
        return ((a * row_number + b) % p) % num_documents

    return hash_function


def min_hash_row(hash_function, row_numbers):
    min_hash = np.iinfo(np.int64).max
    for row in row_numbers:
        hash = hash_function(row)
        if hash < min_hash:
            min_hash = hash

    return min_hash


sig_matrix_hash_functions = list()
band_hash_functions = list()

for i in range(0, num_hash_functions):
    sig_matrix_hash_functions.append(generate_hash_function())

for i in range(0, num_bands * num_sig_matrix_rows):
    band_hash_functions.append(generate_hash_function())

sig_matrix_hash_functions = np.asarray(sig_matrix_hash_functions)
band_hash_functions = np.asarray(band_hash_functions)


def mapper(key, value):
    # key: None
    # value: one line of input file
    tokens = value.split()
    doc_id = tokens[0]
    row_numbers = map(int, tokens[1:])
    sig_col = list()

    # generating signature matrix column for this document
    for curr_hash_function in sig_matrix_hash_functions:
        sig_col.append(min_hash_row(curr_hash_function, row_numbers))

    rows_per_band = num_hash_functions / num_bands

    # hash each "strip" in each band for this document
    for band_number in range(0, num_bands):
        current_sum = 0
        current_strip = list()

        for r in range(band_number * rows_per_band, (band_number + 1) * rows_per_band):
            current_sum += band_hash_functions[band_number * rows_per_band + r](int(sig_col[r]))

        #current_sum += min_hash_row(band_hash_functions[band_number * rows_per_band + r], current_strip)
        yield(hash(int(current_sum % num_documents) * band_number), doc_id)


def reducer(key, values):
    # key: key from mapper used to aggregate
    # values: list of all value for that key
    for i in range(0, len(values)):
        for j in range(i + 1, len(values)):
            yield(int(values[i][6:]), int(values[j][6:]))

