from lib.heap_fixed import FixedHeap
from lib.helpers import build_db_fields_from_csv

file_name = 'anime_db.txt'
table_name = 'animes_short_name'
blocking_factor = 30
fields = build_db_fields_from_csv(csv_file_path='test_file.csv')
fixed_heap = FixedHeap(file_name, table_name, blocking_factor, fields)
fixed_heap.create_register_file()
fixed_heap.populate_from_csv_file('test_file.csv', ',')
fixed_heap.single_select('title', 'Hajime no Ippo')
# select based on list of non-sequential values
fixed_heap.select_all('num_episodes', [11, 12, 20], all_between=False)
# select based on range of values
fixed_heap.select_all('score', [7.00, 7.20], all_between=True)
fixed_heap.single_insert('MyCustomAnime!', 99, 9.01,
                         2023, "2023-01-01", "2023-06-01")
fixed_heap.bulk_insert([['MyCustomAnime2', 97, 8.01, 2023, "2023-06-02", "2023-06-03"],
                        ['MyCustomAnime3', 98, 7.01, 2023, "2023-07-01", "2023-07-02"]])
# fixed_heap.single_select('title', 'MyCustomAnime!')
fixed_heap.single_delete('title', 'MyCustomAnime!')
# will return not found
fixed_heap.single_select('title', 'MyCustomAnime!')
# # will insert on empty space
fixed_heap.single_insert('MyCustomAnime!', 99, 9.01,
                         2023, "2023-01-01", "2023-06-01")
