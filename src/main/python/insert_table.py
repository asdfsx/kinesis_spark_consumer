import kudu

client = kudu.connect(host='127.0.0.1', port=7051)
table = client.table("example")
session = self.client.new_session()
for i in range(nrows):
    op = table.new_insert((i, i*2, 'hello_%d' % i))
    session.apply(op)
session.flush()