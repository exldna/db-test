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

## How to run benchmark

Every bench folder contains:
- `run.sh` file, that runs benchmark once on the fresh machine 
    with preinstalled database management system.
- `results` folder, that contains preivous bench results.

So, the easiest way to run the bench is to following this steps:
1) Start and run fresh docker container.
2) Copy `run.sh` file and `data` folder into the container.
3) Run the shell script.
4) Save results if intended.

If you looking for automated way to run benchmakrs,
please wisit the `README.md` in the main branch.
