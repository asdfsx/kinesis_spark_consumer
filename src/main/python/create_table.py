import kudu

client = kudu.connect(host='127.0.0.1', port=7051)

builder = kudu.schema_builder()
builder.add_column('key', kudu.int32, nullable=False)
builder.add_column('int_val', kudu.int32)
builder.add_column('string_val', kudu.string, default='nothing')
builder.add_column('unixtime_micros_val', kudu.unixtime_micros)
builder.set_primary_keys(['key'])
schema = builder.build()

partitioning = kudu.client.Partitioning().set_range_partition_columns(['key'])

result = client.create_table("example", schema, partitioning)
print result