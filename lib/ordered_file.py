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
    EXTENSION_TABLE_THRESHOLD,
    FIRST_REGISTER_LENGTH,
    MAX_HEADER_COLUMNS,
    MAX_REGISTERS_LENGTH,
    MAX_SPACE_POINTERS,
    MAX_SIZE_EXTENSION_TABLE,
    NEXT_AVALIABLE_LENGTH,
    POINTERS_INDEX,
    TIMESTAMP_LENGTH,
)

logging.basicConfig(level=logging.INFO)


class OrderedFile:
    def __init__(
        self,
        file_name: str,
        table_name: str,
        blocking_factor: int,
        fields_info: Dict,
        sort_column: str,
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
        self.extension_file = 'extension.txt'
        self.sort_column = sort_column

    def _build_create_table(self) -> str:
        create_table_str = f'CREATE TABLE {self.table_name} (' + '\n'
        for column, col_type, size in zip(self.column_names, self.register_types,
                                          self.register_sizes):
            create_table_str += f'{column} {col_type}({size}),\n'
        return create_table_str

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

    def _check_extension_file_size(self) -> int:
        amount = read_and_decode(file_name=self.extension_file,
                                 start=0, end=MAX_SIZE_EXTENSION_TABLE)
        return int(amount)

    def _check_file(self) -> bool:
        if os.path.exists(self.file_name):
            return True
        return False

    def _format_multiple_results(self, registers: str) \
            -> pd.DataFrame:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        sizes = [int(i.split(':')[1]) for i in result]
        # consider the break line character as a column
        # sizes.append(1)
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

    def _format_select_result(self, register: str) -> pd.DataFrame:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        # exclude logical_byte column
        sizes = [int(i.split(':')[1]) for i in result]
        it = iter(register)
        sliced = [list(islice(it, 0, i)) for i in sizes]
        data = {name: [''.join(item)]
                for name, item in zip(self.column_names, sliced)}
        df = pd.DataFrame(data=data)
        df.drop(columns=['logical_byte'], inplace=True)
        return df

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

    def _get_column_sizes(self) -> List[int]:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        sizes = [int(i.split(':')[1]) for i in result]
        return sizes

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

    def _get_desired_value(self, start, end) -> str:
        return read_and_decode(self.file_name, int(start), int(end))

    def _get_size_of_register(self) -> int:
        result = read_and_decode(
            self.file_name, 0, MAX_HEADER_COLUMNS).split(';')[:-1]
        return sum([int(i.split(':')[1]) for i in result])

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

    def _make_header(self) -> None:
        if self._check_file():
            return
        header_text = self._build_header_string()
        with open(file=self.file_name, mode='w+b') as f:
            f.write(bytearray(header_text, 'utf-8'))

    def _merge_extension_table(self, order_field: str) -> None:
        register_size = self._get_size_of_register()
        extension_size = self._check_extension_file_size()
        amount_on_main_file = int(self._get_value_from_field('amount'))
        first_register_pos = self._get_value_from_field('first_register')
        # +1 to account for the ; character
        registers = read_and_decode(self.extension_file, MAX_SIZE_EXTENSION_TABLE+1,
                                    999999)
        # transform in list and split the registers
        registers = list(registers[i:i+register_size] for i
                         in range(0, extension_size*register_size, register_size))
        # remove deleted registers
        registers = [register for register in registers if register[0] == 'Y']
        extension_size = len(registers)
        # short circuit if everything on extension table was to be deleted
        if not registers:
            logging.info('Nothing to do!')
            return
        # sort the list based on desired field
        column_sizes = self._get_column_sizes()
        try:
            desired_col_index = self.column_names.index(order_field)
        except ValueError as e:
            raise Exception(f'Column {order_field} does not exists.') from e

        size_until_desired_col = sum(column_sizes[:desired_col_index])
        start_pos = size_until_desired_col
        end_pos = start_pos + column_sizes[desired_col_index]
        registers.sort(key=lambda x: x[start_pos:end_pos])
        # save the desired column values for easier comparison on next step
        desired_column_values = [x[start_pos:end_pos] for x in registers]
        logging.info(
            f'Preparing to merge {len(registers)} registers to main file.')
        # create new file to insert registers ordered by desired order_field
        with open(file='temp_file.txt', mode='w+b') as f:
            header_text = self._build_header_string()
            f.write(bytearray(header_text, 'utf-8'))
        # iterate on old and registers from extension file
        # and insert every register sorted by the desired column
        # while at it, do not insert registers with logical byte N
        extension_index = 0
        extension_value = desired_column_values[extension_index]
        new_amount = 0
        main_index = 0
        temp = open(file='temp_file.txt', mode='r+b')
        temp.seek(int(first_register_pos))
        with open(file=self.file_name, mode='r+b') as f:
            f.seek(int(first_register_pos))
            current = f.read(register_size)
            decoded = current.decode()
            while main_index < amount_on_main_file \
                    or extension_index < len(registers):
                # check logical byte value
                if decoded[0] == 'N':
                    # logically deleted, can be skipped
                    main_index += 1
                    current = f.read(register_size)
                    decoded = current.decode()
                    continue
                # check if the extension file current value is lower or not
                # or if extension table is already empty
                try:
                    curr_main_value = decoded[start_pos:end_pos]
                except IndexError:
                    # reached the end of main file
                    curr_main_value = ''
                if (curr_main_value <= extension_value or
                    extension_index == len(registers)) \
                        and main_index < amount_on_main_file:
                    temp.write(current)
                    new_amount += 1
                    main_index += 1
                    current = f.read(register_size)
                    decoded = current.decode()
                # keep inserting extension table registers until
                # main register is "lower" or extension table is empty
                else:
                    while (extension_value < curr_main_value or curr_main_value == '') \
                            and extension_index < len(registers):
                        temp.write(
                            bytearray(registers[extension_index], 'utf-8'))
                        new_amount += 1
                        extension_index += 1
                        if extension_index == len(registers):
                            continue
                        extension_value = desired_column_values[extension_index]
            temp.write(b' ')
        temp.close()
        # delete old file
        directory = os.path.dirname(os.path.realpath(self.file_name))
        old_file_path = os.path.join(directory, self.file_name)
        os.remove(old_file_path)
        # rename new file to old file name
        os.rename(os.path.join(directory, 'temp_file.txt'), old_file_path)
        # update desired fields
        self._update_desired_fields(['timestamp', 'amount'], [1, new_amount])
        logging.info(
            f'Successfully merged {len(registers)} registers from extension file.')
        # clear extension table
        with open(file=self.extension_file, mode='w+b') as f:
            f.write(
                bytearray(f'0{" "*(MAX_SIZE_EXTENSION_TABLE - 1)}; ', 'utf-8'))

    def _scan_single_key(self, pk_value: str, target_col: str,
                         table: str = 'main', silenced: bool = True):
        if table == 'main':
            initial_pos = self._get_value_from_field('first_register')
            amount = int(self._get_value_from_field('amount'))
            file = self.file_name
        else:
            initial_pos = MAX_SIZE_EXTENSION_TABLE + 1
            amount = self._check_extension_file_size()
            file = self.extension_file
        result, pointer = self._scan_single_routine(pk_value, target_col, initial_pos,
                                                    amount, file, silenced)
        return result, pointer

    def _scan_all_keys(self, values: str, target_col: str,
                       table: str = 'main', silenced: bool = True,
                       all_between: bool = False):
        if table == 'main':
            initial_pos = self._get_value_from_field('first_register')
            amount = int(self._get_value_from_field('amount'))
            file = self.file_name
        else:
            initial_pos = MAX_SIZE_EXTENSION_TABLE + 1
            amount = self._check_extension_file_size()
            file = self.extension_file
        result, pointer, total_found = \
            self._scan_all_routine(values, target_col, initial_pos, amount,
                                   file, silenced, all_between)
        return result, pointer, total_found

    def _scan_single_routine(self, pk_value: str, target_col: str,
                             initial_pos: str, amount: int, file_name: str,
                             silenced: bool = True) -> Tuple[str, int]:
        cont, pointer = 0, 0
        result = ''
        pk_value = str(pk_value)
        column_size, size_till_column, total_size = \
            self._get_column_and_total_value(target_col)
        with open(file=file_name, mode='r+b') as f:
            f.seek(int(initial_pos))
            while cont <= amount:
                # check if it is logically deleted
                # read specific column to check for equality
                logical_byte = f.read(1).decode()
                f.seek(size_till_column - 1, 1)
                temp_result = f.read(column_size).decode().strip(' ')
                if temp_result == pk_value and logical_byte == 'Y':
                    if not silenced:
                        logging.info('Register found!')
                    # if found, read full register
                    f.seek(-(size_till_column + column_size), 1)
                    result = f.read(total_size).decode().strip(' ')
                    break
                cont += 1
                f.seek(total_size - column_size - size_till_column, 1)
            pointer += f.tell() - total_size
        return result, pointer

    def _scan_all_routine(self, values: str, target_col: str, initial_pos: str,
                          amount: int, file_name: str, silenced: bool = True,
                          all_between: bool = False) -> Tuple[str, int, int]:
        total_found, cont, pointer = 0, 0, 0
        result = ''
        values = [str(val) for val in values]
        column_type = self._get_column_type(target_col)
        column_size, size_till_column, total_size = \
            self._get_column_and_total_value(target_col)
        with open(file=file_name, mode='r+b') as f:
            f.seek(int(initial_pos))
            while cont < amount:
                # check if it is logically deleted
                # read specific column to check for equality
                logical_byte = f.read(1).decode()
                f.seek(size_till_column - 1, 1)
                temp_result = f.read(column_size).decode().strip(' ')
                if (temp_result in values or
                    (check_between(temp_result, values, column_type) and all_between)) \
                        and logical_byte == 'Y':
                    if not silenced:
                        logging.info('Register found!')
                    # if found, read full register
                    f.seek(-(size_till_column + column_size), 1)
                    a = f.read(total_size).decode().strip(' ')
                    result += a
                    cont += 1
                    total_found += 1
                    continue
                cont += 1
                f.seek(total_size - column_size - size_till_column, 1)
            pointer += f.tell() - total_size
        return result, pointer, total_found

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

    def _update_extension_table_amount(self, current_value: int,
                                       amount: int) -> None:
        with open(file=self.extension_file, mode='r+b') as f:
            f.write(bytearray(str(amount + current_value), 'utf-8'))

    def _write_on_end(self, records: List, log_info: bool = False) -> None:
        registers, amount = records
        with open(file=self.extension_file, mode='r+b') as f:
            f.seek(-1, 2)
            for register in registers:
                f.write(register)
        # need to update the amount of records on file
        if log_info:
            logging.info(f'{amount} register(s) added!')

    def create_register_files(self) -> None:
        logging.info('Creating database file...')
        self._make_header()
        with open(file=self.extension_file, mode='w+b') as f:
            # write the amount of registers on extension file atm
            f.write(
                bytearray(f'0{" "*(MAX_SIZE_EXTENSION_TABLE - 1)}; ', 'utf-8'))
        logging.info('Database file created!')

    def single_insert(self, *args) -> int:
        result = 'Y'  # first byte is for logical deletion if necessary
        for info in args:
            result += str(info)
        # this space will be consumed for the next write
        result += ' '
        byte_result = bytearray(result, 'utf-8')
        self._write_on_end([[byte_result], 1], True)

        amount_on_extension_table = self._check_extension_file_size()
        # need to update the extension table counter
        self._update_extension_table_amount(current_value=amount_on_extension_table,
                                            amount=1)
        # check if we need to reorder main file,
        # incorporating extension file records
        if amount_on_extension_table + 1 > EXTENSION_TABLE_THRESHOLD:
            # merge extension file with the main one
            logging.info(
                'Extension table threshold reached.'
                'Preparing to merge with the main file.')
            self._merge_extension_table(self.sort_column)

        return 1

    def bulk_insert(self, registers: List[List]) -> int:
        total_registers = len(registers)
        write_list = [bytearray('Y' + convert_list_to_str(
            item), encoding='utf-8') for item in registers[:-1]]
        # last item needs to have a space at the end, thats why we dont go
        # until the end on the line above
        write_list.append(bytearray('Y' + convert_list_to_str(
            registers[-1]) + ' ', encoding='utf-8'))
        self._write_on_end([write_list, total_registers])
        logging.info(f'{total_registers} register(s) added!')
        # check if we need to reorder main file,
        # incorporating extension file records
        amount_on_extension_table = self._check_extension_file_size()
        self._update_extension_table_amount(current_value=amount_on_extension_table,
                                            amount=total_registers)
        if amount_on_extension_table + total_registers > EXTENSION_TABLE_THRESHOLD:
            # merge extension file with the main one
            logging.info(
                'Extension table threshold reached.'
                'Preparing to merge with the main file.')
            self._merge_extension_table(self.sort_column)
        return 1

    def single_select(self, pk_col: str, pk_value: Any) -> None:
        result, _ = self._scan_single_key(pk_value, pk_col, 'main', False)
        ext_result, _ = self._scan_single_key(pk_value, pk_col,
                                              'extension', False)
        if result == '' and ext_result == '':
            logging.info(f'The value {pk_value} '
                         f'does not exists on column {pk_col}.')
        else:
            if result:
                pretty_result = self._format_select_result(result)
            else:
                pretty_result = self._format_select_result(ext_result)
            print(pretty_result)

    def select_all(self, target_col: str,
                   values: List[Any], all_between: bool = False) -> None:
        records, _, total_found = self._scan_all_keys(
            values, target_col, 'main', True, all_between)
        records_extension, _, total_found_extension = self._scan_all_keys(
            values, target_col, 'extension', True, all_between)
        # records, _, total_found = self._scan_file_for_values(
        #     target_col, values, all_between, True)
        grand_total = total_found + total_found_extension
        print(f'Found {grand_total} records satisfying the conditions.')
        # pretty print
        if grand_total > 0:
            all_records = records + records_extension
            final = self._format_multiple_results(all_records)
            # add column to indicate location of register
            final['file_location'] = \
                ['main']*total_found + ['extension']*total_found_extension
            # drop logical byte column
            final.drop(columns=['logical_byte'], inplace=True)
            print(final.head(grand_total))

    def single_delete(self, pk_col: str, pk_value: str) -> None:
        # find pointer to start of register and get total register size
        result, pointer = self._scan_single_key(pk_value, pk_col, 'main', True)
        result_extend, pointer_extended = \
            self._scan_single_key(pk_value, pk_col, 'extension', True)
        # check if register exists
        if result == '' and result_extend == '':
            logging.info(f'Register with value {pk_value} on column {pk_col}'
                         f' does not exists.')
            return
        # change logical byte to 'N'
        # deleted is on main file
        if result:
            file_name = self.file_name
            final_pointer = pointer
        # deleted is on extension file
        else:
            file_name = self.extension_file
            final_pointer = pointer_extended
        with open(file=file_name, mode='r+b') as f:
            f.seek(final_pointer)
            f.write(bytearray('N', 'utf-8'))
        logging.info('Record deleted!')

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
