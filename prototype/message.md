Topic:
```
messages.public.messages
```

Key:
```json
{"id":"74cb6b55-9a09-4c6d-8b94-8d9b63dd10a2"}
```

Value:
```json5
{
  "before": { // null in case of insert
    "id": "74cb6b55-9a09-4c6d-8b94-8d9b63dd10a2",
    "create_time": "2025-10-26T23:47:21.465566Z",
    "message": "message1",
    "username": "user"
  },
  "after": { // null in case of delete
    "id": "74cb6b55-9a09-4c6d-8b94-8d9b63dd10a2",
    "create_time": "2025-10-26T23:47:21.465566Z",
    "message": "message2",
    "username": "user"
  },
  "source": {
    "version": "3.0.0.Final",
    "connector": "postgresql",
    "name": "messages",
    "ts_ms": 1761523267598,
    "snapshot": "false",
    "db": "defaultdb",
    "sequence": "[\"23967824\",\"23968848\"]",
    "ts_us": 1761523267598210,
    "ts_ns": 1761523267598210000,
    "schema": "public",
    "table": "messages",
    "txId": 755,
    "lsn": 23968848,
    "xmin": null
  },
  "transaction": null,
  "op": "u", // i - insert, u - update, d - delete
  "ts": "2025-10-26T23:47:21.465566Z",
  "ts_ms": 1761523268027,
  "ts_us": 1761523268027183,
  "ts_ns": 1761523268027183807
}
```
