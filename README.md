# Zagreb: A DynamoDB-Compatible Database (WIP)

Zagreb is an experimental, lightweight, and DynamoDB-compatible database implemented in Go. It features a distributed architecture with a central router and multiple storage nodes, utilizing `bbolt` for its underlying storage. This project aims to provide a simplified, local-first database solution that mimics the core API patterns of AWS DynamoDB.

**Disclaimer:** This project is currently a Work In Progress (WIP) and is intended for learning and experimentation. It is not production-ready.

## Architecture

Zagreb operates on a distributed model consisting of two primary components:

-   **Router:** The central entry point for all client requests. It maintains a registry of available storage nodes and is responsible for routing incoming DynamoDB API calls to the appropriate node.
-   **Node:** A storage unit responsible for handling a subset of the data. Each node runs its own instance of the DynamoDB-compatible API and manages a local `bbolt` database file. Nodes register themselves with the router upon startup.

This design allows for horizontal scaling by adding more nodes to the cluster.

## Features

- **DynamoDB-like API:** Implements a subset of DynamoDB's API operations.
- **Distributed Design:** A central router manages and forwards requests to one or more storage nodes.
- **Key-Value Storage:** Leverages `bbolt` for efficient embedded key-value storage on each node.
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

To run Zagreb, you must start the router and at least one node.

1.  **Start the Router:
    Open a terminal and run the following command. The router listens on port `8081`.
    ```bash
    go run cmd/router/main.go
    ```

2.  **Start a Node:
    Open a second terminal and run the following command. This will start a node that listens on port `8001` and registers itself with the router.
    ```bash
    go run cmd/node/main.go
    ```
    The node will create a `node-1.db` file in the project root to store its data. You can run multiple nodes, but you will need to modify the `nodeID` and `nodeAddr` constants in `cmd/node/main.go` to avoid conflicts.

## HTTP API Usage

The API mimics DynamoDB's HTTP API. You can interact with it by sending requests to the **router** on port `8081`. All requests should be `POST` requests to the root path (`/`) and include the `X-Amz-Target` header to specify the operation.

### Example: CreateTable

```bash
curl -X POST 
  http://localhost:8081/ 
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
  http://localhost:8081/ 
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
  http://localhost:8081/ 
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
  http://localhost:8081/ 
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
  http://localhost:8081/ \
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
  http://localhost:8081/ \
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
  http://localhost:8081/ 
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
  http://localhost:8081/ 
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
│   ├───node/
│   │   └───main.go       # Entry point for a storage node
│   └───router/
│       └───main.go       # Entry point for the router
└───pkg/
    ├───api/              # Core API server implementation
    │   ├───api_test.go
    │   └───server.go
    ├───expression/       # Handles DynamoDB-like expression parsing
    │   ├───expression_test.go
    │   └───expression.go
    ├───nodeapi/          # Client for node-to-node communication
    │   └───client.go
    ├───router/           # Router logic for request handling and node management
    │   ├───router_test.go
    │   └───router.go
    ├───routerapi/        # Types for router-node communication
    │   └───types.go
    ├───storage/          # Storage interface and implementations
    │   ├───storage.go
    │   └───bbolt/
    │       ├───bbolt_test.go
    │       └───bbolt.go
    └───types/            # Common DynamoDB data structures
        └───types.go
```

## Contributing

Contributions are welcome! Please feel free to open issues or pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
