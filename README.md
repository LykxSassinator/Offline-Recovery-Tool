# Offline Recovery Tool

Generate offline unsafe recovery scripts from TiKV region metadata logs.

## Usage

```bash
# Step 1: Build the binary
cargo build

# Step 2: Analyze region meta logs and generate recovery scripts
./target/debug/offline-recovery-tool
```

The output scripts will be placed in the current working directory (details depend on implementation).

## Requirements

Before running the tool, ensure:

- Place all region metadata files exported via TiKV Control (see: [Get region metas](https://docs.pingcap.com/zh/tidb/stable/tikv-control/#%E6%9F%A5%E7%9C%8B-raft-%E7%8A%B6%E6%80%81%E6%9C%BA%E7%9A%84%E4%BF%A1%E6%81%AF)) into `./regions/`.
- Run the binary from the directory that contains the `./regions/` folder.
- Name each file using the pattern `<identifier>-regions.log`  
  Example: `store-1-regions.log`, `store-42-regions.log`.

Expected layout:

```
./regions/
  store-1-regions.log
  store-2-regions.log
  store-42-regions.log
```

## Notes

- Generated scripts are for last-resort offline unsafe recovery. Review thoroughly before execution.
- Use only in a controlled, isolated, offline environment.