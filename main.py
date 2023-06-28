from lib.heap_fixed import FixedHeap
from time import sleep

file_name = 'teste_db.txt'
table_name = 'first_table'
blocking_factor = 30
fields = {
    'name': {
        'type': 'CHAR',
        'size': 9
    },
    'age': {
        'type': 'INTEGER',
        'size': 2
    },
    'occupation': {
        'type': 'CHAR',
        'size': 8
    },
    'cpf': {
        'type': 'CHAR',
        'size': 11
    }
}
fixed_heap = FixedHeap(file_name, table_name, blocking_factor, fields)
fixed_heap.create_register_file()
# single insert
fixed_heap.single_insert('Alhaitham', '25', 'akscribe', '11111111112')
# just to change the last_updated field
sleep(3)
fixed_heap.single_insert('Kaedehara', '30', 'traveler', '11111111113')
# select based on pk_col field
fixed_heap.single_select('name', 'Alhaitham')
# sinlge delete based for any column and any value (will delete first occurence only)
fixed_heap.single_delete('occupation', 'akscribe')
# bulk insert (will use empty spaces first)
fixed_heap.bulk_insert([['Alhaitham', '25', 'akscribe', '11111111112'],
                        ['StarGanyu', '50', 'teacher+', '11111111114']])

fixed_heap.bulk_insert([['KeqingQix', '17', 'quixing+', '11111111115'],
                        ['Kamisato+', '18', 'warrior+', '11111111116']])
# select based on list of non-sequential values
fixed_heap.select_all('occupation', ['traveler', 'akscribe', 'teacher+'],
                      all_between=False)
# select based on a range of values
fixed_heap.select_all('age', ['17', '45'], all_between=True)
