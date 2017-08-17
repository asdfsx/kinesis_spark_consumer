import kudu

client = kudu.connect(host='127.0.0.1', port=7051)
result = client.list_tables()
for table_name in result:
    table_obj = client.table(table_name)
    print table_obj.schema