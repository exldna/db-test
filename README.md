# Databases workload comparison

## Schema definition

```schema
User:
    addr: 25-32 String
    txs: [Transaction]

Transaction:
    hash: 128 bits in hex
    time: Timestamp

'Majority' rule:
    50% of Transactions belongs to 1% of Users

Queries:
    - bulk insert: [User] -> ()
        inserts a lot of data in the one request
    - select nth: User.addr, n -> [N; Transaction]
        returns nth block of user transactions
```

## How to generate data

```bash
git checkout dev

cargo install --path ./crates/tools --bin generate_data
```

```bash
$ generate_data --help

# example usage
$ generate_data ./data 1000 2000 3000
```
