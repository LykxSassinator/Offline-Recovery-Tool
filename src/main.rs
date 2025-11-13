use chrono::{DateTime, offset::Local};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::io::{Read, Write};
use std::time;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Peer {
    id: u64,
    store_id: u64,
    start_key: String,
    end_key: String,
    conf_ver: u64,
    version: u64,
    store_ids: Vec<u64>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Region {
    id: u64,
    peers: Vec<Peer>,
    start_key: String,
    end_key: String,
    conf_ver: u64,
    version: u64,
    store_ids: Vec<u64>,
}

fn read_regions(store_id_to_address: &HashMap<u64, &str>) -> HashMap<u64, Region> {
    let mut regions = HashMap::<u64, Region>::new();
    let mut buffer = vec![];
    for (store_id, addr) in store_id_to_address.iter() {
        println!("开始加载 {}", addr);
        let mut count = 0;
        let ip = addr[..addr.len() - 6].to_string();
        let mut f = fs::File::open(format!("./regions/{}-regions.log", ip)).unwrap();
        let metadata = fs::metadata(format!("./regions/{}-regions.log", ip)).unwrap();
        buffer.clear();
        if metadata.len() + 10 > buffer.capacity() as u64 {
            buffer.reserve(metadata.len() as usize + 10);
        }
        f.read_to_end(&mut buffer)
            .expect("Could not read region file");
        let data: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        println!("json 加载完毕 {}", addr);
        for (region_id_key, peer_value) in data["region_infos"].as_object().unwrap().iter() {
            if !peer_value
                .as_object()
                .unwrap()
                .contains_key("raft_apply_state")
                || peer_value["raft_apply_state"].is_null()
            {
                continue;
            }
            if !peer_value
                .as_object()
                .unwrap()
                .contains_key("raft_local_state")
                || peer_value["raft_local_state"].is_null()
            {
                continue;
            }
            let region_id = region_id_key.parse::<u64>().unwrap();
            if !regions.contains_key(&region_id) {
                regions.insert(
                    region_id,
                    Region {
                        id: region_id,
                        peers: Vec::with_capacity(3),
                        start_key: String::new(),
                        end_key: String::new(),
                        conf_ver: 0,
                        version: 0,
                        store_ids: Vec::with_capacity(3),
                    },
                );
            }
            let region = regions.get_mut(&region_id).unwrap();

            let peer = peer_value["region_local_state"]["region"]
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
                start_key: peer["start_key"].to_string(),
                end_key: peer["end_key"].to_string(),
                conf_ver: peer["region_epoch"]["conf_ver"].as_u64().unwrap(),
                version: peer["region_epoch"]["version"].as_u64().unwrap(),
                store_ids,
            });

            count += 1;
            if count % 100000 == 0 {
                println!("已加载 {} 条", count);
            }
        }
        println!("加载完成 {}", addr);
    }
    drop(buffer);
    println!("共加载 {} 个 Region", regions.len());
    println!("开始保存 Region 整理文件");
    let dump = bincode::serialize(&regions).unwrap();
    fs::write("./regions_no_sort.bin", dump).unwrap();
    println!("完成保存 Region 整理文件");
    regions
}

fn read_regions_from_bin() -> HashMap<u64, Region> {
    let mut f = fs::File::open("./regions_no_sort.bin").unwrap();
    let metadata = fs::metadata("./regions_no_sort.bin").unwrap();
    let mut buffer = Vec::with_capacity(metadata.len() as usize + 10);
    f.read_to_end(&mut buffer)
        .expect("Could not read region file");
    println!("已读取整理文件");
    let result = bincode::deserialize(&buffer).unwrap();
    println!("已加载整理文件");
    result
}

fn sort_regions(regions: HashMap<u64, Region>) -> Vec<Region> {
    println!("开始预处理");
    let mut count = 0;
    let mut result = vec![];
    for (region_id, mut region) in regions {
        for peer in region.peers.iter() {
            if peer.version >= region.version && peer.conf_ver >= region.conf_ver {
                if peer.version == region.version && peer.conf_ver == region.conf_ver {
                    let mut equal = region.start_key == peer.start_key
                        && peer.end_key == region.end_key
                        && region.store_ids.len() == peer.store_ids.len();
                    for (i, store_id) in region.store_ids.iter().enumerate() {
                        if !equal {
                            break;
                        }
                        equal = equal && *store_id == peer.store_ids[i];
                    }
                    if !equal {
                        println!("ver 相同但数据不一致，id: {}", region_id);
                    }
                } else {
                    region.start_key = peer.start_key.clone();
                    region.end_key = peer.end_key.clone();
                    region.conf_ver = peer.conf_ver;
                    region.version = peer.version;
                    region.store_ids = peer.store_ids.clone();
                }
                continue;
            } else if peer.version < region.version && peer.conf_ver < region.conf_ver {
                continue;
            } else {
                println!("无法判断大小，id: {}", region_id);
            }
        }
        result.push(region);
        count += 1;
        if count % 100000 == 0 {
            println!("已处理 {} 条", count);
        }
    }
    println!("开始排序");
    result.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    println!("结束排序，开始保存 Region 排序文件");
    let dump = bincode::serialize(&result).unwrap();
    fs::write("./regions_sort.bin", dump).unwrap();
    println!("完成保存 Region 排序文件");
    result
}

fn sort_regions_from_bin() -> Vec<Region> {
    let mut f = fs::File::open("./regions_sort.bin").unwrap();
    let metadata = fs::metadata("./regions_sort.bin").unwrap();
    let mut buffer = Vec::with_capacity(metadata.len() as usize + 10);
    f.read_to_end(&mut buffer)
        .expect("Could not read region file");
    println!("已读取排序文件");
    let result = bincode::deserialize(&buffer).unwrap();
    println!("已加载排序文件");
    result
}

fn analyze(regions: &mut Vec<Region>) -> Vec<Region> {
    let mut need_recmp_count = 0;
    let mut need_select_count = 0;
    let mut auto_count = 0;
    let mut healthy_count = 0;
    let mut count = 0;
    let mut end_key = "".to_string();
    let mut error_regions = vec![];
    for region in regions.iter_mut() {
        for peer in region.peers.iter() {
            if peer.version > region.version || peer.conf_ver > region.conf_ver {
                region.start_key = peer.start_key.clone();
                region.end_key = peer.end_key.clone();
                region.conf_ver = peer.conf_ver;
                region.version = peer.version;
                region.store_ids = peer.store_ids.clone();
            }
        }
        let mut diff = false;
        for peer in region.peers.iter() {
            if peer.version > region.version || peer.conf_ver > region.conf_ver {
                need_recmp_count += 1;
                diff = false;
                break;
            }
            if peer.version == region.version && peer.conf_ver == region.conf_ver {
                let mut equal = region.start_key == peer.start_key
                    && peer.end_key == region.end_key
                    && region.store_ids.len() == peer.store_ids.len();
                for (i, store_id) in region.store_ids.iter().enumerate() {
                    if !equal {
                        break;
                    }
                    equal = equal && *store_id == peer.store_ids[i];
                }
                if !equal {
                    if need_select_count == 0 {
                        println!("{}", serde_json::to_string(region).unwrap());
                    }
                    need_select_count += 1;
                    diff = false;
                    break;
                }
            }
            if peer.version != region.version || peer.conf_ver != region.conf_ver {
                diff = true;
            }
        }
        if diff {
            error_regions.push(region.clone());
            auto_count += 1;
        } else {
            healthy_count += 1;
        }

        if end_key != region.start_key {
            if end_key < region.start_key {
                println!("Region 空洞 {} {}", end_key, region.start_key);
            } else {
                println!("Region 重叠 {} {}", region.start_key, end_key);
            }
        }
        end_key = region.end_key.clone();

        count += 1;
        if count % 100000 == 0 {
            println!("已处理 {} 条", count);
        }
    }

    serde_json::to_writer_pretty(
        fs::File::create("./error_regions.json").unwrap(),
        &error_regions,
    )
    .unwrap();
    println!("Region Count: {}", regions.len());
    println!("need_recmp_count: {}", need_recmp_count);
    println!("need_select_count: {}", need_select_count);
    println!("auto_count: {}", auto_count);
    println!("healthy_count: {}", healthy_count);
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

// check a - b == c
fn check_vec_eq(a: &Vec<u64>, b: &Vec<u64>) -> bool {
    let mut res = a.clone();
    let mut res2 = b.clone();
    res.sort();
    res2.sort();
    if res.len() != res2.len() {
        return false;
    }
    for (i, s) in res.iter().enumerate() {
        if res2[i] != *s {
            return false;
        }
    }
    true
}

fn gen_shell(error_regions: &Vec<Region>, store_id_to_address: &HashMap<u64, &str>) {
    let t = time::SystemTime::now();
    let now: DateTime<Local> = t.into();

    let mut operatef = fs::File::create("./operate.txt").unwrap();
    let mut shellf =
        fs::File::create(format!("./all_shell_{}.sh", now.format("%Y%m%d%H%M"))).unwrap();

    let mut result = HashMap::<u64, Fix>::new();
    for region in error_regions.iter() {
        writeln!(&mut operatef, "region: {}", region.id).unwrap();
        let mut need_remove = vec![];
        let mut need_leave = vec![];
        for peer in region.peers.iter() {
            if peer.version != region.version || peer.conf_ver != region.conf_ver {
                need_remove.push(peer.store_id);
            } else {
                need_leave.push(peer.store_id);
            }
        }
        for peer in region.peers.iter() {
            let fix = result.entry(peer.store_id).or_insert_with(|| Fix {
                store_id: peer.store_id,
                recover: HashMap::new(),
                tombstone: vec![],
            });
            if peer.version != region.version || peer.conf_ver != region.conf_ver {
                fix.tombstone.push(region.id);
                writeln!(&mut operatef, "store {} tombstone", peer.store_id).unwrap();
            } else {
                let mut new_ids = vec![];
                for store_id in peer.store_ids.iter() {
                    if need_remove.contains(store_id) || !need_leave.contains(store_id) {
                        let fix_recover = fix.recover.entry(*store_id).or_insert_with(|| vec![]);
                        fix_recover.push(region.id);
                        writeln!(
                            &mut operatef,
                            "store {} unsafe recover {}",
                            peer.store_id, store_id
                        )
                        .unwrap();
                    } else {
                        new_ids.push(*store_id);
                    }
                }
                if !check_vec_eq(&new_ids, &need_leave) {
                    println!("有内鬼，终止交易");
                }
            }
        }
        writeln!(&mut operatef).unwrap();
    }

    writeln!(&mut shellf, "# Region 异常修复脚本").unwrap();
    writeln!(&mut shellf, "# 生成时间: {}", now.format("%Y-%m-%d %H:%M")).unwrap();
    writeln!(&mut shellf).unwrap();

    let mut fixs = result.into_values().collect::<Vec<_>>();
    fixs.sort_by(|a, b| a.store_id.cmp(&b.store_id));
    for mut fix in fixs {
        let addr = store_id_to_address.get(&fix.store_id).unwrap();
        let ip = addr[..addr.len() - 6].to_string();
        writeln!(shellf, "# 修复 Store ID: {}", fix.store_id).unwrap();
        writeln!(shellf, "# {}", ip).unwrap();
        fix.tombstone.sort();
        if fix.tombstone.len() > 0 {
            writeln!(
                shellf,
                "tikv-ctl --data-dir /data/tidb/data/tikv-20160/ tombstone -r {} --force",
                vec_u64_join(&fix.tombstone)
            )
            .unwrap();
        }
        let mut store_ids = fix.recover.keys().map(|x| *x).collect::<Vec<_>>();
        store_ids.sort();
        for store_id in store_ids {
            let fix_recover = fix.recover.get_mut(&store_id).unwrap();
            fix_recover.sort();
            writeln!(shellf, "tikv-ctl --data-dir /data/tidb/data/tikv-20160/ unsafe-recover remove-fail-stores -s {} -r {}", store_id, vec_u64_join(fix_recover)).unwrap();
        }
        writeln!(
            shellf,
            "echo \"已完成 Store {} (Host {}) 的 region 修复\"",
            fix.store_id, ip
        )
        .unwrap();
        writeln!(
            shellf,
            "echo \"============================================================\""
        )
        .unwrap();
        writeln!(shellf).unwrap();
    }
}

fn main() {
    let store_id_to_address: HashMap<u64, &str> = HashMap::from([
        (1, "10.13.108.103:20160"),
        (2, "10.13.102.44:20160"),
        (3, "10.13.108.212:20160"),
        (4, "10.13.96.66:20160"),
        (5, "10.13.103.247:20160"),
        (6, "10.13.105.25:20160"),
        (7, "10.13.105.144:20160"),
        (12, "10.13.104.116:20160"),
        (20, "10.13.101.174:20160"),
        (24, "10.13.102.101:20160"),
        (27, "10.13.99.228:20160"),
        (28, "10.13.101.236:20160"),
        (30634626, "10.65.77.56:20160"),
        (30634629, "10.65.175.251:20160"),
        (30634630, "10.65.134.53:20160"),
        (30634631, "10.65.149.147:20160"),
        (79074496, "10.65.41.25:20160"),
        (79074563, "10.65.100.30:20160"),
    ]);
    // Step 1: read and get all region metas
    let regions_no_sort = read_regions(&store_id_to_address);
    let mut regions = sort_regions(regions_no_sort);

    // Step 1.optional: optional, if you've already generated the data file of all regions
    // you can annotate the above "step 1" and directly execute "step 2".
    //
    // let regions_no_sort = read_regions_from_bin();
    // let mut regions = sort_regions_from_bin();

    // Step 2: analyze abnormal regions.
    let error_regions = analyze(&mut regions);
    // Step 3: generate shell scripts for offline recovery
    gen_shell(&error_regions, &store_id_to_address);
}