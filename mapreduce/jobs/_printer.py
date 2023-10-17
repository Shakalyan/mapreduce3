def print_separator(columns_max):
    for mx in columns_max:
        print('+', end='')
        print('-'*(mx+2), end='')
    print('+')


def print_data(columns_max, data):
    for i in range(len(data)):
        print('| ', end='')
        print(data[i], end='')
        print(' '*(columns_max[i] - len(data[i])+1), end='')
    print('|')


def print_table(tuples):
    columns = []
    columns_max = []
    for key in tuples[0].keys():
        columns.append(key)
        columns_max.append(len(key))
    
    for tuple in tuples:
        for i in range(len(columns)):
            columns_max[i] = max(columns_max[i], len(tuple[columns[i]]))
    
    print_separator(columns_max)
    print_data(columns_max, columns)
    print_separator(columns_max)

    for tuple in tuples:
        values = []
        for column in columns:
            values.append(tuple[column])
        print_data(columns_max, values)
    
    print_separator(columns_max)