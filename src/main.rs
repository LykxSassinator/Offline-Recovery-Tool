use chrono::{DateTime, offset::Local};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::io::{Read, Write};
use std::time;

const STORE_ID_TO_ADDRESS: [(u64, &str); 4] = [
    (1, "10.11.123.1:20160"),
    (2, "10.11.123.2:20160"),
    (3, "10.11.123.3:20160"),
    (4, "10.11.123.4:20160"),
];

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Peer {
    id: u64,
    store_id: u64,
    start_key: String,
    end_key: String,
    conf_ver: u64,
    version: u64,
    store_ids: Vec<u64>, // 这个 Peer 节点认为此 Region 在哪些 Store 上有 Peer
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Region {
    id: u64,
    peers: Vec<Peer>,
    // 根据 Peer 信息预测的 Region 合理状态
    start_key: String,
    end_key: String,
    conf_ver: u64,
    version: u64,
    store_ids: Vec<u64>,
}

fn read_regions() -> HashMap<u64, Region> {
    // 如果已经缓存，则直接读取
    let path = "./cache/regions_tidy.bin";
    if fs::exists(path).unwrap() {
        let mut f = fs::File::open(path).unwrap();
        let metadata = fs::metadata(path).unwrap();
        let mut buffer = Vec::with_capacity(metadata.len() as usize + 10);
        f.read_to_end(&mut buffer).unwrap();
        println!("已读取 Regions 整理文件");
        let result = bincode::deserialize(&buffer).unwrap();
        println!("已加载 Regions 整理文件");
        return result;
    }

    let store_id_to_address: HashMap<u64, &str> = HashMap::from(STORE_ID_TO_ADDRESS);
    let mut regions = HashMap::<u64, Region>::new();
    let mut buffer = vec![];
    for (store_id, addr) in store_id_to_address.iter() {
        let ip = addr[..addr.len() - 6].to_string();
        let path = format!("./regions/{}-regions.json", ip);

        // 读取 json 文件
        println!(
            "开始读取 Regions    store_id: {} address: {}",
            store_id, addr
        );
        let mut f = fs::File::open(&path).unwrap();
        let metadata = fs::metadata(&path).unwrap();
        buffer.clear();
        if metadata.len() + 10 > buffer.capacity() as u64 {
            buffer.reserve(metadata.len() as usize + 10);
        }
        f.read_to_end(&mut buffer).unwrap();
        let data: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        println!("读取完毕");

        // 从 json 文件中加载有用的 Region 信息
        let mut count = 0;
        for store_peer_value in data["region_infos"].as_object().unwrap().values() {
            let store_peer = store_peer_value.as_object().unwrap();

            // 如果没有 --skip-tombstone 参数，则输出文件中会包含无效 Peer，需要通过如下方式忽略
            // 在 tikv-ctl 6.1 版本上，--skip-tombstone 是无法正常工作的
            if !store_peer.contains_key("raft_apply_state")
                || store_peer["raft_apply_state"].is_null()
                || !store_peer.contains_key("raft_local_state")
                || store_peer["raft_local_state"].is_null()
            {
                continue;
            }

            // 读取这个 Store 的 Peer 信息，存入对应 Region 中
            let region_id = store_peer["region_id"].as_u64().unwrap();
            let region = regions.entry(region_id).or_insert_with(|| Region {
                id: region_id,
                peers: Vec::with_capacity(3),
                start_key: String::new(),
                end_key: String::new(),
                conf_ver: 0,
                version: 0,
                store_ids: Vec::with_capacity(3),
            });

            let peer = store_peer["region_local_state"]["region"]
                .as_object()
                .unwrap();
            let mut store_ids = Vec::with_capacity(3);
            for store in peer["peers"].as_array().unwrap() {
                store_ids.push(store["store_id"].as_u64().unwrap());
            }
            store_ids.sort();
            region.peers.push(Peer {
                id: peer["id"].as_u64().unwrap(),
                store_id: *store_id,
                start_key: peer["start_key"].as_str().unwrap().to_string(),
                end_key: peer["end_key"].as_str().unwrap().to_string(),
                conf_ver: peer["region_epoch"]["conf_ver"].as_u64().unwrap(),
                version: peer["region_epoch"]["version"].as_u64().unwrap(),
                store_ids,
            });

            count += 1;
            if count % 100000 == 0 {
                println!("已加载 {} 条", count);
            }
        }

        println!("加载完成 共 {} 条", count);
    }
    drop(buffer);

    println!("开始保存 Region 整理文件");
    let dump = bincode::serialize(&regions).unwrap();
    fs::write(path, dump).unwrap();
    println!("完成保存 Region 整理文件");
    regions
}

fn sort_regions(regions: HashMap<u64, Region>) -> Vec<Region> {
    // 如果已经缓存，则直接读取
    let path = "./cache/regions_sort.bin";
    if fs::exists(path).unwrap() {
        let mut f = fs::File::open(path).unwrap();
        let metadata = fs::metadata(path).unwrap();
        let mut buffer = Vec::with_capacity(metadata.len() as usize + 10);
        f.read_to_end(&mut buffer).unwrap();
        println!("已读取 Regions 排序文件");
        let result = bincode::deserialize(&buffer).unwrap();
        println!("已加载 Regions 排序文件");
        return result;
    }

    println!("开始对 Region Peer 进行挑选");
    let mut count = 0;
    let mut result = Vec::with_capacity(regions.len());
    for (_, mut region) in regions {
        for peer in region.peers.iter() {
            if peer.version > region.version || peer.conf_ver > region.conf_ver {
                region.start_key = peer.start_key.clone();
                region.end_key = peer.end_key.clone();
                region.conf_ver = peer.conf_ver;
                region.version = peer.version;
                region.store_ids = peer.store_ids.clone();
            }
        }
        result.push(region);
        count += 1;
        if count % 100000 == 0 {
            println!("已处理 {} 条", count);
        }
    }
    println!("完成 Region Peer 挑选");

    println!("开始对 Region 按 Start Key 排序");
    result.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    println!("结束排序");
    println!("开始保存 Region 排序文件");
    let dump = bincode::serialize(&result).unwrap();
    fs::write(path, dump).unwrap();
    println!("完成保存 Region 排序文件");
    result
}

fn analyze(regions: Vec<Region>) -> Vec<Region> {
    println!("开始分析错误 Region");

    let mut need_recmp_count = 0; // 存在不同的 Peer 各自的 conf_ver 的 version 更大的情况
    let mut need_select_count = 0; // 存在满足最大 conf_ver 以及 version 的 Peer 有多个，且不一致的情况
    let mut error_count = 0; // 最大 conf_ver 以及 version 的 Peer 只有一个，或者多个但信息一致，但有不匹配的其他 Peer
    let mut healthy_count = 0; // 所有 Peer 的 meta 信息均一致
    let mut count = 0;

    let mut end_key = "".to_string();
    let mut error_regions = vec![];
    for region in regions {
        let mut is_error = false;
        for peer in region.peers.iter() {
            if peer.version > region.version || peer.conf_ver > region.conf_ver {
                need_recmp_count += 1;
                is_error = true;
                break;
            } else if peer.version == region.version && peer.conf_ver == region.conf_ver {
                let mut equal = region.start_key == peer.start_key
                    && peer.end_key == region.end_key
                    && region.store_ids.len() == peer.store_ids.len();
                for (i, store_id) in region.store_ids.iter().enumerate() {
                    equal = equal && *store_id == peer.store_ids[i];
                }
                if !equal {
                    need_select_count += 1;
                    is_error = true;
                    break;
                }
            } else {
                is_error = true;
            }
        }

        // 判断 range 是否连续
        if end_key != region.start_key {
            if end_key < region.start_key {
                println!("Region 空洞 \"{}\" \"{}\"", end_key, region.start_key);
            } else {
                println!("Region 重叠 \"{}\" \"{}\"", region.start_key, end_key);
            }
        }
        end_key = region.end_key.clone();

        // 保留错误 Region
        if is_error {
            error_regions.push(region);
            error_count += 1;
        } else {
            healthy_count += 1;
        }

        count += 1;
        if count % 100000 == 0 {
            println!("已处理 {} 条", count);
        }
    }

    if end_key != "" {
        println!("Region 空洞 \"{}\" \"\"", end_key);
    }

    // 错误数据保存到文件
    serde_json::to_writer_pretty(
        fs::File::create("./error_regions.json").unwrap(),
        &error_regions,
    )
    .unwrap();

    println!("完成分析错误 Region");
    println!("need recmp: {}", need_recmp_count);
    println!("need select: {}", need_select_count);
    println!(
        "auto fix: {}",
        error_count - need_recmp_count - need_select_count
    );
    println!("healthy: {}", healthy_count);

    error_regions
}

struct Fix {
    store_id: u64,
    recover: HashMap<u64, Vec<u64>>,
    tombstone: Vec<u64>,
}

fn vec_u64_join(v: &Vec<u64>) -> String {
    v.iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn check_sorted_vec_eq(a: &Vec<u64>, b: &Vec<u64>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for i in 0..a.len() {
        if a[i] != b[i] {
            return false;
        }
    }
    true
}

fn gen_shell(error_regions: &Vec<Region>) {
    println!("开始生产自动修复方案");

    let store_id_to_address: HashMap<u64, &str> = HashMap::from(STORE_ID_TO_ADDRESS);

    let now: DateTime<Local> = time::SystemTime::now().into();

    let mut operate_f = fs::File::create("./operate.txt").unwrap(); // 实际操作文件，用于检查

    let mut result = HashMap::<u64, Fix>::new();
    for region in error_regions.iter() {
        writeln!(&mut operate_f, "region: {}", region.id).unwrap();

        // 决策需要保留的 Peer
        let mut need_leave = vec![];
        for peer in region.peers.iter() {
            if peer.version == region.version && peer.conf_ver == region.conf_ver {
                need_leave.push(peer.store_id);
            }
        }
        need_leave.sort();

        // 针对每个 Peer 进行二次 check 并生成调整方案
        // 调整方案会按照 Store 进行合并
        for peer in region.peers.iter() {
            // 获取 Peer 所在 Store 的 Fix
            let fix = result.entry(peer.store_id).or_insert_with(|| Fix {
                store_id: peer.store_id,
                recover: HashMap::new(),
                tombstone: vec![],
            });

            if peer.version != region.version || peer.conf_ver != region.conf_ver {
                // 如果 Peer 自身 meta 与 Region 状态不符，则进行 tombstone
                fix.tombstone.push(region.id);
                writeln!(&mut operate_f, "store {} tombstone", peer.store_id).unwrap();
            } else {
                // 如果 Peer 自身 meta 与 Region 状态相符，则在其保存的 meta 信息中，移除掉有问题的其他 Peer
                let mut new_ids = vec![];
                for store_id in peer.store_ids.iter() {
                    // 可能会包含实际并不存在的 Peer 的信息，也需要删除
                    if !need_leave.contains(store_id) {
                        let fix_recover = fix.recover.entry(*store_id).or_insert_with(|| vec![]);
                        fix_recover.push(region.id);
                        writeln!(
                            &mut operate_f,
                            "store {} unsafe recover {}",
                            peer.store_id, store_id
                        )
                        .unwrap();
                    } else {
                        new_ids.push(*store_id);
                    }
                }
                if !check_sorted_vec_eq(&new_ids, &need_leave) {
                    println!(
                        "ERROR 修复完毕之后 Peer meta 仍不一致   剩余：{:?}    需要：{:?}",
                        new_ids, need_leave
                    );
                }
            }
        }
        writeln!(&mut operate_f).unwrap();
    }

    // 根据 Fix 信息生成脚本文件
    let mut shell_f =
        fs::File::create(format!("./all_shell_{}.sh", now.format("%Y%m%d%H%M"))).unwrap();
    writeln!(&mut shell_f, "# Region 异常修复脚本").unwrap();
    writeln!(&mut shell_f, "# 生成时间: {}", now.format("%Y-%m-%d %H:%M")).unwrap();
    writeln!(&mut shell_f).unwrap();

    let mut fixes = result.into_values().collect::<Vec<_>>();
    fixes.sort_by(|a, b| a.store_id.cmp(&b.store_id));
    for mut fix in fixes {
        let addr = store_id_to_address.get(&fix.store_id).unwrap();
        let ip = addr[..addr.len() - 6].to_string();
        writeln!(shell_f, "# 修复 Store ID: {}", fix.store_id).unwrap();
        writeln!(shell_f, "# {}", ip).unwrap();
        // tombstone 部分
        fix.tombstone.sort();
        if fix.tombstone.len() > 0 {
            writeln!(
                shell_f,
                "tikv-ctl --data-dir /data/tidb/data/tikv-20160/ tombstone -r {} --force",
                vec_u64_join(&fix.tombstone)
            )
            .unwrap();
        }
        // unsafe-recover 部分
        let mut store_ids = fix.recover.keys().map(|x| *x).collect::<Vec<_>>();
        store_ids.sort();
        for store_id in store_ids {
            let fix_recover = fix.recover.get_mut(&store_id).unwrap();
            fix_recover.sort();
            writeln!(shell_f, "tikv-ctl --data-dir /data/tidb/data/tikv-20160/ unsafe-recover remove-fail-stores -s {} -r {}", store_id, vec_u64_join(fix_recover)).unwrap();
        }
        writeln!(
            shell_f,
            "echo \"已完成 Store {} (Host {}) 的 region 修复\"",
            fix.store_id, ip
        )
        .unwrap();
        writeln!(
            shell_f,
            "echo \"============================================================\""
        )
        .unwrap();
        writeln!(shell_f).unwrap();
    }
}

fn main() {
    if !fs::exists("./cache").unwrap() {
        fs::create_dir("./cache").unwrap();
    }
    let regions_tidy = read_regions();
    println!("共加载 {} 个 Region", regions_tidy.len());
    let regions = sort_regions(regions_tidy);
    let error_regions = analyze(regions);
    gen_shell(&error_regions);
}