```sh
cargo run -- index ~/hdfs-logs-multitenants-10000.json ~/test.index --merge
# Commiting 10000 documents, after processing 10000
# Merging 3 segments
# Joining merging threads
# Writing unified index file

cargo run -- search ~/test.index "severity_text:INFO" --limit 1 | jq .
# {
#   "_dynamic": [
#     {
#       "attributes": {
#         "class": "org.apache.hadoop.hdfs.server.datanode.DataNode"
#       },
#       "body": "Receiving BP-108841162-10.10.34.11-1440074360971:blk_1074072706_331882 src: /10.10.34.33:42666 dest: /10.10.34.11:50010",
#       "resource": {
#         "service": "datanode/01"
#       },
#       "severity_text": "INFO",
#       "tenant_id": 46
#     }
#   ],
#   "timestamp": [
#     "2016-04-13T06:46:54Z"
#   ]
# }
```
