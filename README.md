Providenciamos um acelerador:

- `build_db_fields_from_csv`: automaticamente reconhece o nome, tipo e tamanho das colunas

```python
from lib.helpers import build_db_fields_from_csv

fields = build_db_fields_from_csv(csv_file_path='your_csv.csv')
```

Retorna um dicionário do formato:

```python
fields = {
    'field_1': {
        'type': 'type_of_field_1',
        'size': size_of_field_1
    },
    'field_2': {
        'type': 'type_of_field_2',
        'size': size_of_field_2
    }
}
```

Esse pode ser passado como parâmetro para a classe `FixedHeap`
para criar seu arquivo de banco de dados.

Além disso, a classe `FixedHeap` providencia o módulo:

- `populate_from_csv_file`: popula o arquivo de base de dados com os dados de um csv passado.

```python
from lib.heap_fixed import FixedHeap
from lib.helpers import build_db_fields_from_csv

file_name = 'desired_db_file_name.txt'
table_name = 'desired_table_name'
blocking_factor = 30
fields = build_db_fields_from_csv(csv_file_path='your_csv.csv')
my_db = FixedHeap(file_name, table_name, blocking_factor, fields)
my_db.create_register_file()
my_db.populate_from_csv_file('your_csv.csv', 'csv_delimiter')
```

Depois de criado o arquivo, e populado, pode começar a usar.
Exemplos usando o arquivo csv `test_file.csv` fornecido.

```python
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
my_db.select_all('score', [7.00, 7.20], all_between=True)
my_db.single_insert(
    'MyCustomAnime!', 99, 9.01, 2023, "2023-01-01", "2023-06-01"
)
my_db.bulk_insert(
    [['MyCustomAnime2', 97, 8.01, 2023,"2023-06-02", "2023-06-03"],
     ['MyCustomAnime3', 98, 7.01, 2023, "2023-07-01", "2023-07-02"]]
)
my_db.single_select('title', 'MyCustomAnime!')
my_db.single_delete('title', 'MyCustomAnime!')
# will return not found
my_db.single_select('title', 'MyCustomAnime!')
# # will insert on empty space
my_db.single_insert(
'MyCustomAnime!', 99, 9.01, 2023, "2023-01-01", "2023-06-01"
)
```

Instalar bibliotecas:

```sh
pip install -r requirements.txt
```

Rodar exemplos de teste:

```sh
python unordered_example.py
```

```sh
python ordered_example.py
```
