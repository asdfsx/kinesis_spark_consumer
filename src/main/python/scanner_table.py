import kudu

client = kudu.connect(host='127.0.0.1', port=7051)
result = client.list_tables()
for table_name in result:
    table_obj = client.table(table_name)
    scanner = table_obj.scanner().open()
    for tuple in scanner.read_all_tuples():
        print tuple
