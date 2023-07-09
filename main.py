import os
from lib.heap_fixed import FixedHeap
from lib.helpers import build_db_fields_from_csv

file_name = 'anime_db.txt'
table_name = 'animes_short_name'
blocking_factor = 30
fields = build_db_fields_from_csv(csv_file_path='test_file.csv')
my_db = FixedHeap(file_name, table_name, blocking_factor, fields)
# only need to run on first time
if not os.path.exists(file_name):
    my_db.create_register_file()
    my_db.populate_from_csv_file('test_file.csv', ',')
my_db.single_select('title', 'Hajime no Ippo')
# select based on list of non-sequential values
my_db.select_all('num_episodes', [11, 12, 20], all_between=False)
# select based on range of values
my_db.select_all('score', [7.00, 7.20], all_between=True)
my_db.single_insert('MyCustomAnime!', 99, 9.01,
                    2023, "2023-01-01", "2023-06-01")
my_db.bulk_insert([['MyCustomAnime2', 97, 8.01, 2023, "2023-06-02", "2023-06-03"],
                   ['MyCustomAnime3', 98, 7.01, 2023, "2023-07-01", "2023-07-02"]])
# will be found
my_db.single_select('title', 'MyCustomAnime!')
# deleting
my_db.single_delete('title', 'MyCustomAnime!')
# will return not found
my_db.single_select('title', 'MyCustomAnime!')
# will insert on empty space
my_db.single_insert('MyCustomAnime!', 99, 9.01,
                    2023, "2023-01-01", "2023-06-01")
# found again
my_db.single_select('title', 'MyCustomAnime!')
