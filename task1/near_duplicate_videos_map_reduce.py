import numpy as np

def mapper(key, value):
    # key: None
    # value: one line of input file
    #if False:
        #yield "key", "value"  # this is how you yield a key, value pair
    tokens = value.split()
    id = tokens[0]
    rows = map(int, tokens[1:])
    sig_col = list()
    for curr_hash_function in hash_function_list_1:
        sig_col.append(hash_row(curr_hash_function, rows))

    print 'sig_col is ' + str(sig_col)

    rows_per_band = num_hash_functions / num_bands
    for i in range(0, num_bands):
        current_sum = 0
        current_strip = list()
        for j in range(i * rows_per_band, (i + 1) * rows_per_band):
            print 'j is ' + str(j)
            current_strip.append(int(sig_col[j]))

        # we now have the current strip
        print 'current strip ' + str(current_strip)
        current_sum += hash_row(hash_function_list_2[i], current_strip)
        yield(id + 'band' + str(i), current_sum % num_documents)





def reducer(key, values):
    # key: key from mapper used to aggregate
    # values: list of all value for that key
    if False:
        yield "key", "value"  # this is how you yield a key, value pair


def generate_hash_function(p=113):
    a = np.random.randint(1, np.iinfo(np.int64).max)
    b = np.random.randint(np.iinfo(np.int64).max)
    print 'a is ' + str(a)
    print 'b is ' + str(b)

    def hash_function(row_number):
        print 'row number is ' + str(row_number)
        return (a * row_number + b) % p

    return hash_function

def hash_row(hash_function, rows):
    # set this to int max?
    min_hash = 32132132131
    for row in rows:
        hash = hash_function(row)
        if hash < min_hash:
            min_hash = hash

    return min_hash

hash_function_list_1 = list()
hash_function_list_2 = list()

num_documents = 100
num_hash_functions = 10
num_bands = 5

for i in range(0, num_hash_functions):
    hash_function_list_1.append(generate_hash_function())

for i in range(0, num_hash_functions):
    hash_function_list_2.append(generate_hash_function())

hash_function_list_1 = np.asarray(hash_function_list_1)
hash_function_list_2 = np.asarray(hash_function_list_2)

for element in mapper(None, "VIDEO_000000000 2310 1916 3585 7404 5278 3976 4392 2773 3691 6126 3831 5152 548 5965 6161 6603 3032 2899 49 7840 134 1831 8093 140 1889 7968 5031 4261 4894 2241 7064 7380 7981 4101 2164 5875 5087 7660 2782 4694 7038 3326 39 852 4063 6843 2521 8011 7569 4118 2578 642 6763 3277 2043 2328 3696 5220 167 7995 273 7664 1892 3151 3719 5432 7740 2549 1387 5870 6027 5595 3759 3263 6711 1912 6135 3650 3016 2728 2294 8134 1140 4673 2247 5957 3482 4918 1615 5138 2730 7530 7711 2537 7124 2397 4749 4807 3744 380"):
    print str(element)
