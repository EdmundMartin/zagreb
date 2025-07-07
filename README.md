# Zagreb: A DynamoDB-Compatible Database (WIP)

Zagreb is an experimental, lightweight, and DynamoDB-compatible database implemented in Go, utilizing `bbolt` for its underlying storage. This project aims to provide a simplified, local-first database solution that mimics the core API patterns of AWS DynamoDB.

**Disclaimer:** This project is currently a Work In Progress (WIP) and is intended for learning and experimentation. It is not production-ready.

## Features

- **DynamoDB-like API:** Implements a subset of DynamoDB's API operations.
- **Key-Value Storage:** Leverages `bbolt` for efficient embedded key-value storage.
- **Basic Operations:
    - `CreateTable`: Define table schemas with primary keys (HASH and RANGE).
    - `PutItem`: Store items in tables.
    - `GetItem`: Retrieve items by primary key.
    - `UpdateItem`: Modify existing items.
    - `DeleteItem`: Remove items from tables.
    - `Query`: Basic querying by hash key.
- **Attribute Value Handling:** Supports DynamoDB-like attribute value types (String, Number, Boolean, Null).

## Getting Started

### Prerequisites

- Go (version 1.23.10 or higher)

### Installation

1.  **Clone the repository:
    ```bash
    git clone https://github.com/your-username/zagreb.git # Replace with actual repo URL
    cd zagreb
    ```

2.  **Download dependencies:
    ```bash
    go mod tidy
    ```

### Running the Application

The `main.go` file now starts an HTTP API server that listens on port `8000`.

```bash
go run cmd/main.go
```

This will create a `my.db` file in the project root directory and start the API server.

## HTTP API Usage

The API mimics DynamoDB's HTTP API. You can interact with it using `curl` or any HTTP client. All requests should be `POST` requests to the root path (`/`) and include the `X-Amz-Target` header to specify the operation.

### Example: CreateTable

```bash
curl -X POST 
  http://localhost:8000/ 
  -H "Content-Type: application/x-amz-json-1.0" 
  -H "X-Amz-Target: DynamoDB_20120810.CreateTable" 
  -d '{
    "TableName": "Users",
    "KeySchema": [
      { "AttributeName": "UserID", "KeyType": "HASH" },
      { "AttributeName": "Timestamp", "KeyType": "RANGE" }
    ],
    "AttributeDefinitions": [
      { "AttributeName": "UserID", "AttributeType": "S" },
      { "AttributeName": "Timestamp", "AttributeType": "N" },
      { "AttributeName": "Email", "AttributeType": "S" }
    ]
  }'
```

### Example: PutItem

```bash
curl -X POST 
  http://localhost:8000/ 
  -H "Content-Type: application/x-amz-json-1.0" 
  -H "X-Amz-Target: DynamoDB_20120810.PutItem" 
  -d '{
    "TableName": "Users",
    "Item": {
      "UserID":    { "S": "user123" },
      "Timestamp": { "N": "1678886400" },
      "Email":     { "S": "user123@example.com" },
      "Name":      { "S": "John Doe" }
    }
  }'
```

### Example: GetItem

```bash
curl -X POST 
  http://localhost:8000/ 
  -H "Content-Type: application/x-amz-json-1.0" 
  -H "X-Amz-Target: DynamoDB_20120810.GetItem" 
  -d '{
    "TableName": "Users",
    "Key": {
      "UserID":    { "S": "user123" },
      "Timestamp": { "N": "1678886400" }
    }
  }'
```

### Example: UpdateItem

```bash
curl -X POST 
  http://localhost:8000/ 
  -H "Content-Type: application/x-amz-json-1.0" 
  -H "X-Amz-Target: DynamoDB_20120810.UpdateItem" 
  -d '{
    "TableName": "Users",
    "Key": {
      "UserID":    { "S": "user123" },
      "Timestamp": { "N": "1678886400" }
    },
    "UpdateExpression": "SET Email = :newEmail",
    "ExpressionAttributeValues": {
      ":newEmail": { "S": "new_email@example.com" }
    }
  }'
```

### Example: Add (for numeric attributes)

```bash
curl -X POST \
  http://localhost:8000/ \
  -H "Content-Type: application/x-amz-json-1.0" \
  -H "X-Amz-Target: DynamoDB_20120810.UpdateItem" \
  -d '{
    "TableName": "Users",
    "Key": {
      "UserID":    { "S": "user123" },
      "Timestamp": { "N": "1678886400" }
    },
    "UpdateExpression": "ADD Age :ageIncrement",
    "ExpressionAttributeValues": {
      ":ageIncrement": { "N": "1" }
    }
  }'
```

### Example: Delete (removes scalar attributes)

```bash
curl -X POST \
  http://localhost:8000/ \
  -H "Content-Type: application/x-amz-json-1.0" \
  -H "X-Amz-Target: DynamoDB_20120810.UpdateItem" \
  -d '{
    "TableName": "Users",
    "Key": {
      "UserID":    { "S": "user123" },
      "Timestamp": { "N": "1678886400" }
    },
    "UpdateExpression": "DELETE Email"
  }'
```

### Example: Query
```

```bash
curl -X POST 
  http://localhost:8000/ 
  -H "Content-Type: application/x-amz-json-1.0" 
  -H "X-Amz-Target: DynamoDB_20120810.Query" 
  -d '{
    "TableName": "Users",
    "KeyConditionExpression": "UserID = \"user123\""
  }'
```

### Example: DeleteItem

```bash
curl -X POST 
  http://localhost:8000/ 
  -H "Content-Type: application/x-amz-json-1.0" 
  -H "X-Amz-Target: DynamoDB_20120810.DeleteItem" 
  -d '{
    "TableName": "Users",
    "Key": {
      "UserID":    { "S": "user123" },
      "Timestamp": { "N": "1678886400" }
    }
  }'
```


## Project Structure

```
zagreb/
├───go.mod                # Go module definition
├───go.sum                # Go module checksums
├───cmd/
│   └───main.go           # Main application entry point
└───pkg/
    ├───expression/       # Handles DynamoDB-like expression parsing and attribute value manipulation
    │   ├───expression_test.go
    │   └───expression.go
    ├───storage/          # Storage interface definition
    │   ├───storage.go
    │   └───bbolt/        # bbolt implementation of the storage interface
    │       ├───bbolt_test.go
    │       └───bbolt.go
    └───types/            # Defines common data structures for requests, responses, and attribute types
        └───types.go
```

## Contributing

Contributions are welcome! Please feel free to open issues or pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details. (Note: A `LICENSE` file needs to be added.)
