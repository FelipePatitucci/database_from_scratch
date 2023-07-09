import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union
import numpy as np
import pandas as pd
from itertools import islice
from .helpers import (
    check_between,
    read_and_decode,
    adjust_digit_counts,
    convert_list_to_str
)
from .configs import (
    AMOUNT_POINTERS_CHARACTERS,
    FIELDS,
    FIRST_REGISTER_LENGTH,
    MAX_HEADER_COLUMNS,
    MAX_REGISTERS_LENGTH,
    MAX_SPACE_POINTERS,
    NEXT_AVALIABLE_LENGTH,
    POINTERS_INDEX,
    TIMESTAMP_LENGTH,
)

logging.basicConfig(level=logging.INFO)


class FixedHeap:
    def __init__(
        self,
        file_name: str,
        table_name: str,
        blocking_factor: int,
        fields_info: Dict,
        # need to calculate position
    ) -> None:
        '''
        table_name: maximum of 32 characters (will be truncated)
        blocking_factor: ideally powers of 2 (at least of size of single register)
        Fields info is a dictionary of the format:
        data = {
            'name_of_column_1': {
                'type': 'type_of_field', (e.g.: CHAR)
                'size': 'max_size_of_field', (e.g.: 32)
            }
        }
        '''
        self.file_name = file_name
        self.table_name = table_name
        self.column_names = [col for col in fields_info]
        self.blocking_factor = blocking_factor
        self.fields_info = fields_info
        self.register_types = [col['type'] for col in fields_info.values()]
        self.register_sizes = [int(col['size'])
                               for col in fields_info.values()]

    def _check_file(self) -> bool:
        if os.path.exists(self.file_name):
            return True
        return False

    def _make_header(self) -> None:
        if self._check_file():
            return
        header_text = self._build_header_string()
        with open(file=self.file_name, mode='w+b') as f:
            f.write(bytearray(header_text, 'utf-8'))

    def _build_header_string(self) -> str:
        pointers, text = self._build_text_and_positions()
        # calculate the final position for each value
        spaces = [MAX_REGISTERS_LENGTH, TIMESTAMP_LENGTH,
                  NEXT_AVALIABLE_LENGTH, FIRST_REGISTER_LENGTH]
        upper_range = [pointer + space for pointer,
                       space in zip(pointers, spaces)]
        new_digits_from_upper_range = sum([len(str(x)) for x in upper_range])
        full_list = []
        for pointer, end_pointer in zip(pointers, upper_range):
            full_list.append(pointer)
            full_list.append(end_pointer)
        # add to each pointer, the extra delimitation character count
        full_list = [pointer + AMOUNT_POINTERS_CHARACTERS
                     for pointer in full_list]
        # new digits from adding delimiters
        new_digits_from_delimiters = sum([len(str(x)) for x in full_list]) - \
            new_digits_from_upper_range

        # build size of each register at the beggining of the file
        columns_idxs = ''
        for idx, size in zip(range(len(self.register_sizes)), self.register_sizes):
            columns_idxs += str(idx) + ':' + str(size) + ';'
        # filling with blank spaces to keep the size for later adding if needed
        columns_idxs += ' '*(MAX_HEADER_COLUMNS - len(columns_idxs))

        # recursively add new digits and shift value if necessary
        new_sizes = adjust_digit_counts(
            full_list,
            new_digits_from_upper_range+new_digits_from_delimiters+MAX_HEADER_COLUMNS)

        pointers_string = self._build_pointers_string(new_sizes)
        # new_sizes[-1] is the size until now, and we add 1
        # because of the line break at the end
        text += str(new_sizes[-1] + 1) + \
            ' '*(FIRST_REGISTER_LENGTH - len(str(new_sizes[-1] + 1))) + '\n '

        return columns_idxs + pointers_string + text

    def _build_pointers_string(self, pointers: List[int]) -> str:
        formated_pointers = ''
        for pointer, end_pointer in zip(pointers[::2], pointers[1::2]):
            formated_pointers += str(pointer) + '-' + str(end_pointer) + ';'
        return formated_pointers

    def _build_create_table(self) -> str:
        create_table_str = f'CREATE TABLE {self.table_name} (' + '\n'
        for column, col_type, size in zip(self.column_names, self.register_types,
                                          self.register_sizes):
            create_table_str += f'{column} {col_type}({size}),\n'
        return create_table_str

    def _build_text_and_positions(self) -> List[Union[List, str]]:
        current_time = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        final_text = ''
        final_text += FIELDS['table_name'] + self.table_name + \
            FIELDS['blocking_factor'] + str(self.blocking_factor) + \
            FIELDS['total_registers']
        amount_pointer = len(final_text)
        final_text += '0' + ' '*(MAX_REGISTERS_LENGTH - 1) + \
            FIELDS['timestamp_creation'] + current_time + \
            FIELDS['timestamp_update']
        timestamp_pointer = len(final_text)
        final_text += current_time + '\n\n' + self._build_create_table() + \
            ')\nNext Free Space:'
        first_space_pointer = len(final_text)
        final_text += '-1' + ' '*14 + '\nFirst Register:'
        first_register_pointer = len(final_text)
        pointers = [amount_pointer, timestamp_pointer,
                    first_space_pointer, first_register_pointer]
        return [pointers, final_text]

    def _update_desired_fields(self, fields: List[str], amounts: List[int]) -> None:
        for field, amount in zip(fields, amounts):
            start, end = self._get_start_and_end_field(field=field)
            curr_value = self._get_desired_value(start, end)
            with open(file=self.file_name, mode='r+b') as f:
                f.seek(int(start))
                if field == 'timestamp':
                    date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    f.write(bytearray(date, 'utf8'))
                elif field == 'amount':
                    f.write(bytearray(str(int(curr_value)+amount), 'utf8'))
                else:
                    str_amount = str(amount)
                    f.write(bytearray(
                        str_amount + ' ' *
                            (FIRST_REGISTER_LENGTH - len(str_amount)),
                            'utf8'))

    def _get_desired_value(self, start, end) -> str:
        return read_and_decode(self.file_name, int(start), int(end))

    def _get_start_and_end_field(self, field: str) -> List[str]:
        pointers_string = read_and_decode(
            self.file_name, MAX_HEADER_COLUMNS, MAX_SPACE_POINTERS + MAX_HEADER_COLUMNS)
        field_range = pointers_string.split(';')[POINTERS_INDEX[field]]
        start, end = field_range.split('-')
        return start, end

    def _get_value_from_field(self, field: str) -> str:
        '''
        Get value from a desired field of the header of the db file.
        '''
        start, end = self._get_start_and_end_field(field)
        return self._get_desired_value(start, end)

    def create_register_file(self) -> None:
        logging.info('Creating database file...')
        self._make_header()
        logging.info('Database file created!')

    def _find_avaliable_spot(self) -> int:
        start, end = self._get_start_and_end_field(field='first_empty')
        curr_value = self._get_desired_value(start, end)
        return int(curr_value)

    def _write_on_end(self, records: List, log_info: bool = False) -> None:
        registers, amount = records
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(-1, 2)
            for register in registers:
                f.write(register)
        if log_info:
            logging.info(f'{amount} register(s) added!')

    def _write_on_free_spot(self, record: str, free_spot: int,
                            log_info: bool = True) -> None:
        next_free = 0
        first_register = int(self._get_value_from_field('first_register'))
        register_size = len(record) - 1  # space at the end
        # dont need to write the space
        result = bytearray(record[:-1], 'utf-8')
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(free_spot, 0)
            next_free = int(f.read(register_size).decode().strip(' '))
            f.seek(-register_size, 1)
            f.write(result)
        # check if we need to update the first_register pointer
        if first_register > free_spot:
            self._update_desired_fields(
                fields=['first_register'], amounts=[free_spot])
        self._update_desired_fields(
            fields=['first_empty'], amounts=[next_free])
        if log_info:
            logging.info('Register added!')

    def single_insert(self, *args) -> int:
        result = ''
        for info in args:
            result += str(info)
        # this space will be consumed for the next write
        result += ' '
        avaliable_spot = self._find_avaliable_spot()
        byte_result = bytearray(result, 'utf-8')
        # no free spot, append on the end
        if avaliable_spot == -1:
            self._write_on_end([[byte_result], 1], True)
        else:
            self._write_on_free_spot(result, avaliable_spot)
        self._update_desired_fields(
            fields=['amount', 'timestamp'], amounts=[1, 1])
        return 1

    def bulk_insert(self, registers: List[List]) -> int:
        free_space = True
        curr_index = 0
        total_registers = len(registers)
        while free_space:
            for _, register_info in enumerate(registers):
                register = ''.join([str(item) for item in register_info])
                # this space will be consumed for the next write
                register += ' '
                avaliable_spot = self._find_avaliable_spot()
                if avaliable_spot == -1:
                    free_space = False
                    # write rest on the end
                    break
                # if we have avaliable spot, write on it
                self._write_on_free_spot(register, avaliable_spot, False)
                curr_index += 1
        if curr_index < total_registers:
            remaining = registers[curr_index:]
            write_list = [bytearray(convert_list_to_str(
                item), encoding='utf-8') for item in remaining[:-1]]
            # last item needs to have a space at the end, thats why we dont go
            # until the end on the line above
            write_list.append(bytearray(convert_list_to_str(
                remaining[-1]) + ' ', encoding='utf-8'))
            self._write_on_end([write_list, total_registers-curr_index])
        self._update_desired_fields(
            fields=['amount', 'timestamp'], amounts=[total_registers, 1])
        logging.info(f'{total_registers} register(s) added!')
        return 1

    def single_select(self, pk_col: str, pk_value: Any) -> None:
        result, _ = self._scan_till_key(pk_col, pk_value)
        if result == '':
            logging.info(f'The value {pk_value} '
                         f'does not exists on column {pk_col}.')
        else:
            pretty_result = self._format_select_result(result)
            print(pretty_result)

    def _format_select_result(self, register: str) -> pd.DataFrame:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        sizes = [int(i.split(':')[1]) for i in result]
        it = iter(register)
        sliced = [list(islice(it, 0, i)) for i in sizes]
        data = {name: [''.join(item)]
                for name, item in zip(self.column_names, sliced)}
        return pd.DataFrame(data=data)

    def _format_multiple_results(self, registers: str) \
            -> pd.DataFrame:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        sizes = [int(i.split(':')[1]) for i in result]
        # consider the break line character as a column
        sizes.append(1)
        total_sizes = sum(sizes)
        cumulative = [0] + list(np.cumsum(np.array(sizes)))
        registers_list = [registers[i:i+total_sizes] for i
                          in range(0, len(registers), total_sizes)]
        registers_spaced = []
        for reg in registers_list:
            registers_spaced += \
                [[reg[ant:curr] for ant, curr
                  in zip(cumulative[0:-1], cumulative[1:])]]
        final_ans = [list(items) for items in zip(*registers_spaced)]
        data = {name: item
                for name, item in zip(self.column_names, final_ans)}
        return pd.DataFrame(data=data)

    def _scan_till_key(self, pk_col: str, pk_value: Any,
                       silenced: bool = False) -> Tuple[str, int]:
        '''
        Scans the db file for register pk_value on column pk_col.
        If it exists, returns the register string.
        Returns blank string otherwise.
        '''
        pk_value = str(pk_value)
        column_size, size_till_column, total_size = self._get_column_and_total_value(
            pk_col)
        initial_pos = self._get_value_from_field('first_register')
        amount = int(self._get_value_from_field('amount'))
        cont, pointer = 0, 0
        result = ''
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(int(initial_pos) + size_till_column)
            while cont <= amount:
                # read specific column to check for equality
                temp_result = f.read(column_size).decode().strip(' ')
                if temp_result == pk_value:
                    if not silenced:
                        logging.info('Register found!')
                    # if found, read full register
                    f.seek(-(size_till_column + column_size), 1)
                    result = f.read(total_size).decode().strip(' ')
                    break
                cont += 1
                f.seek(total_size - column_size, 1)
            pointer += f.tell() - total_size
        return result, pointer

    def _scan_file_for_values(self, target_col: str, values: List[Any],
                              all_between: bool = False,
                              silenced: bool = False) -> Tuple[str, int]:
        '''
        Scans the db file for registers that match one of the value
        on 'values' on the column 'target_col'.
        If it exists, returns the register strings.
        Returns blank string otherwise.
        '''
        values = [str(val) for val in values]
        column_size, size_till_column, total_size = \
            self._get_column_and_total_value(target_col)
        initial_pos = self._get_value_from_field('first_register')
        amount = int(self._get_value_from_field('amount'))
        # column_type = self._get_column_type(target_col)
        cont, pointer, total_found = 0, 0, 0
        final_result = ''
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(int(initial_pos) + size_till_column)
            while cont <= amount:
                # read specific column to check for equality
                result = f.read(column_size).decode().strip(' ')
                if result in values or \
                        (check_between(result, values) and all_between):
                    if not silenced:
                        logging.info('Register found!')
                    # if found, read full register
                    f.seek(-(size_till_column + column_size), 1)
                    final_result += f.read(total_size).decode().strip(' ') + '\n'
                    total_found += 1
                    f.seek(size_till_column, 1)
                    continue
                cont += 1
                f.seek(total_size - column_size, 1)
            pointer += f.tell() - total_size
        return final_result, pointer, total_found

    def _get_column_type(self, target_col: str) -> str:
        result = ''
        for col_name, col_type in zip(self.column_names, self.register_types):
            if col_name == target_col:
                result = col_type
                break
        if not result:
            logging.warning(f'The column {target_col} does not exists!')
            raise ValueError
        return result

    def select_all(self, target_col: str,
                   values: List[Any], all_between: bool = False) -> None:
        records, _, total_found = self._scan_file_for_values(
            target_col, values, all_between, True)
        print(f'Found {total_found} records satisfying the conditions.')
        # pretty print
        final = self._format_multiple_results(records)
        print(final.head(total_found))

    def single_delete(self, pk_col: str, pk_value: str) -> None:
        # find pointer to start of register and get total register size
        result, pointer = self._scan_till_key(pk_col, pk_value, True)
        register_size = self._get_size_of_register()
        # check if register exists
        if result == '':
            logging.info(f'Register with value {pk_value} on column {pk_col}'
                         f' does not exists.')
            return
        # get current empty spot
        current_empty = int(self._get_value_from_field('first_empty'))
        # write blank spaces and pointer to next deleted record (if exists)
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(pointer)
            if current_empty == -1:
                f.write(bytearray('-1' + ' '*(register_size-2), 'utf-8'))
                self._update_desired_fields(fields=['first_empty'],
                                            amounts=[pointer])
            else:
                to_write = str(current_empty)
                f.write(bytearray(to_write + ' ' *
                        (register_size - len(to_write)), 'utf-8'))
                self._update_desired_fields(fields=['first_empty'],
                                            amounts=[pointer])
        # update remaining header fields
        self._update_desired_fields(fields=['amount', 'timestamp'],
                                    amounts=[-1, 0])
        # check if we removed the very first register. If so, we need to update
        # the value on 'first register' pointer
        current_empty = int(self._get_value_from_field('first_empty'))
        current_first_register = int(
            self._get_value_from_field('first_register'))
        if current_empty == current_first_register:
            self._update_desired_fields(fields=['first_register'],
                                        amounts=[current_first_register+register_size])
        logging.info('Record deleted!')

    def _get_size_of_register(self) -> int:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        return sum([int(i.split(':')[1]) for i in result])

    def _get_column_and_total_value(self, column: str) -> Tuple[int, int, int]:
        header_str = read_and_decode(self.file_name, 0, MAX_HEADER_COLUMNS)
        pairs = header_str.strip().split(';')[:-1]
        try:
            desired_col_pair = pairs[self.column_names.index(column)]
        except ValueError:
            logging.error(f'Column {column} does not exists on db file.')
            raise
        column_idx, column_size = desired_col_pair.split(':')
        total_size_till_column = sum([int(value.split(':')[1])
                                      for value in pairs[:int(column_idx)]])
        total_register_size = sum([int(value.split(':')[1])
                                  for value in pairs])
        return int(column_size), total_size_till_column, total_register_size

    def populate_from_csv_file(self, file_path: str, separator: str = ',',
                               max_lines: int = 1000) -> None:
        total_lines = 0
        final_text = ''
        register_size = self._get_size_of_register()
        logging.info(f'Loading data from file {file_path}')
        with open(file_path, 'r+', encoding='utf-8') as f:
            for line in f:
                if total_lines == max_lines + 1:
                    # max line per iteration reached
                    break
                # skip header
                if total_lines == 0:
                    total_lines += 1
                    continue
                text = ''.join(line[:-1].split(separator))
                # this register has some character that does not belong
                # to regular 127 ASCII table, so it exceeds the
                # register max size, cannot add to database
                if len(text.encode("utf-8")) > register_size:
                    logging.warning(
                        f'The register {text} cannot be added because'
                        f' it uses a character that does not belongs to ASCII 127.')
                    continue
                final_text += ''.join(line[:-1].split(separator))
                total_lines += 1
            final_text += ' '
        # write the registers
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(-1, 2)
            f.write(bytearray(final_text, 'utf-8'))
        logging.info(
            f'Populated database with {total_lines - 1} records from {file_path}!')
        self._update_desired_fields(
            fields=['amount', 'timestamp'], amounts=[total_lines - 1, 1])
