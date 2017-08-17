import kudu

client = kudu.connect(host='127.0.0.1', port=7051)
result = client.list_tables()
print result