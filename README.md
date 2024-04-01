# Database Connection Classes
Database connection classes for different dbs.


---
## Dependencies
Make sure `psycopg2` and `pandas` are installed before using the classes using these commands (Mac & Linux):
```shell
pip list | grep psycopg2
```
```shell
pip list | grep pandas
```
Or on Windows:
```shell
pip list | findstr psycopg2
```
```shell
pip list | findstr pandas
```

You should get outputs like this with the package name and it's version:
```
>> psycopg2                  2.9.9
```
```
>> pandas                    2.2.1
```

If nothing shows up that means the specific package is not installed. Use these commands to install the required packages:
```shell
pip install psycopg2
```
```shell
pip install pandas
```


## Usage
Copy the directory into your base project directory if you want to use it as a module and import it.
```python
from db_connections import PostgresConnection, SQLiteConnection
```

Or just copy the code into your codebase to use them directly.
