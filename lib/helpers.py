from typing import Any, Dict, List


def read_and_decode(file_name: str, start: str,
                    end: str) -> str:
    '''
    Reads from a binary string file and returns a portion of it,
    decoded to normal string.
    '''
    result = ''
    amount = end - start
    with open(file=file_name, mode='r+b') as f:
        f.seek(start)
        result = f.read(amount).decode().strip(' ')
    return result


def adjust_digit_counts(input_list: List[int], value: int) -> List[int]:
    '''
    Verifies, for each number in input_list, if by adding 'value'
    the number will gain one extra digit. If so, return the amount
    of new digits after the operation.
    Do this until no new extra digits appears.
    '''
    if value == 0:
        return input_list
    new_res = [item+value for item in input_list]
    new_value = len(convert_list_to_str(new_res)) - \
        len(convert_list_to_str(input_list))
    return adjust_digit_counts(new_res, new_value)


def convert_list_to_str(input_list: List[int]) -> str:
    return ''.join([str(i) for i in input_list])


def check_between(result: str, values: List[Any]) -> bool:
    return (result <= max(values)) and (result >= min(values))


def build_db_fields_from_csv(csv_file_path: str, separator: str = ',') -> Dict:
    lines = []
    with open(csv_file_path, 'r+', encoding='utf-8') as f:
        cont = 0
        for line in f:
            lines.append(line[:-1])  # ignore break line
            cont += 1
            if cont == 2:
                break
    columns_names, row_example = lines
    names = {title: {'type': '', 'size': 0} for title
             in columns_names.split(separator)}
    for name, value in zip(names, row_example.split(separator)):
        if value.isdigit():
            # integer value
            names[name]['type'] = 'INTEGER'
            names[name]['size'] = len(value)
        # check if can be a float
        # if (bool(re.search(r'^\d+?\.\d+?$', value)))
        elif value.replace('.', '', 1).isdigit():
            # can be a numeric column
            names[name]['type'] = 'FLOAT'
            names[name]['size'] = len(value)
        # cannot be a numeric column
        else:
            names[name]['type'] = 'CHAR'
            names[name]['size'] = len(value)
    return names
