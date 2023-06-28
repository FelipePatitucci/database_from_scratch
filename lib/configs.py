FIELDS = {
    'table_name': 'Table:',
    'blocking_factor': ' BF:',
    'total_registers': ' Amount:',
    'timestamp_creation': 'Created at:',
    'timestamp_update': ' Last modified:'
}
POINTERS_INDEX = {
    'amount': 0,
    'timestamp': 1,
    'first_empty': 2,
    'first_register': 3
}
MAX_HEADER_COLUMNS = 200
MAX_SPACE_POINTERS = 64
MAX_REGISTERS_LENGTH = 16
TIMESTAMP_LENGTH = 19
NEXT_AVALIABLE_LENGTH = 16
FIRST_REGISTER_LENGTH = 16
AMOUNT_POINTERS_CHARACTERS = 8  # 2xtotal de variaveis 'LENGTH'