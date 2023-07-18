from lib.ordered_file import OrderedFile
from lib.helpers import build_db_fields_from_csv
import os

if __name__ == '__main__':
    file_name = 'testes.txt'
    table_name = 'animes_short_name'
    blocking_factor = 30
    fields = build_db_fields_from_csv(csv_file_path='test_file.csv',
                                      logical_deletion=True)
    my_db = OrderedFile(file_name, table_name,
                        blocking_factor, fields, 'title')
    # only need to run on first time
    if not os.path.exists(file_name):
        my_db.create_register_files()

    # threshold for extension table is set at 4 on config,
    # so this will not trigger the merge operation
    # on extension file, registers are not yet ordered
    my_db.bulk_insert([['Bakemonogatari', 15, 8.33,
                        2009, '2009-07-03', '2010-06-25'],
                       ['Great Mazinger', 56, 7.06,
                        1974, '1974-09-08', '1975-09-28'],
                       ['Dragon Ball GT', 64, 6.47,
                        1996, '1996-02-07', '1997-11-19'],
                       ['Black Cat (TV)', 23, 7.33,
                        2005, '2005-10-07', '2006-03-31']])
    # you can see on column 'file_location' that they are still on
    # the extension file, hence are not ordered by title yet
    # select also searches extension file
    my_db.select_all('score', [0, 10], all_between=True)
    # this insert will trigger the merge, since now it will have 5 registers
    my_db.single_insert('Hajime no Ippo', 75, 8.75,
                        2000, '2000-10-04', '2002-03-27')
    # notice that all registers are now sorted by title and located on main file
    my_db.select_all('score', [0, 10], all_between=True)
    # testing a variety of selects
    my_db.single_select('title', 'Hajime no Ippo')
    my_db.single_select('num_episodes', 23)
    my_db.single_select('score', 6.47)
    my_db.single_select('airing_date', '1974-09-08')
    # will not be found
    my_db.single_select('score', 9.99)
    # select all on list of non-sequential values
    my_db.select_all('num_episodes', [64, 23, 15], all_between=False)
    # you can also use names that are not on file
    my_db.select_all('title', ['Akira', 'HunterxHunter'], all_between=True)
    # the deletion operation is a logical one
    # it changes the logical byte to 'N' and on the next merge,
    # the registers gets removed
    my_db.single_delete('title', 'Great Mazinger')
    # will not show on select anymore
    my_db.select_all('score', [0, 10], True)
    my_db.single_insert('Zettai Shounen', 26, 6.87,
                        2005, '2005-05-21', '2005-11-19')
    my_db.single_insert('Fuyu no Sonata', 26, 7.28,
                        2009, '2009-10-17', '2010-05-01')
    my_db.single_insert('Pandora Hearts', 25, 7.66,
                        2009, '2009-04-03', '2009-09-25')
    my_db.single_insert('Dragon Crisis!', 12, 6.64,
                        2011, '2011-01-11', '2011-03-29')
    my_db.single_delete('title', 'Fuyu no Sonata')
    # will trigger merge again
    my_db.single_insert('Kamisama Dolls', 13, 7.01,
                        2011, '2011-07-06', '2011-09-28')
    # will stay on extension file until next merge
    my_db.single_insert('Hanasaku Iroha', 26, 7.91,
                        2011, '2011-04-03', '2011-09-25')
    # notice that neither Great Mazinger nor Fuyu no Sonata shows up
    # they are also gone from main file
    my_db.select_all('score', [0, 10], True)
