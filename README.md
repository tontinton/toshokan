```sh
cargo run -- create example_config.yaml

cargo run -- index test ~/hdfs-logs-multitenants-10000.json
# Commiting 10000 documents, after processing 10000
# Merging 3 segments
# Joining merging threads
# Writing unified index file

cargo run -- search test "severity_text:INFO" --limit 1 | jq .
# {
#   "attributes": {
#     "class": "org.apache.hadoop.hdfs.server.datanode.DataNode.clienttrace"
#   },
#   "body": "src: /10.10.34.30:33078, dest: /10.10.34.11:50010, bytes: 234, op: HDFS_WRITE, cliID: DFSClient_NONMAPREDUCE_-202827006_103, offset: 0, srvID: d9ef1b17-4314-4cd8-91eb-095413c3427f, blockid: BP-108841162-10.10.34.11-1440074360971:blk_1074072709_331885, duration: 2571934",
#   "resource": {
#     "service": "datanode/01"
#   },
#   "severity_text": "INFO",
#   "tenant_id": 61,
#   "timestamp": "2016-04-13T06:46:54Z"
# }

cargo run -- drop test
```
