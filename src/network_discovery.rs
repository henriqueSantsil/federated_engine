use crate::server::RemoteView;
use if_addrs::IfAddr;
use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

const PORT: u16 = 8080;
const MAX_HOSTS_PER_SUBNET: u64 = 4094;
const WORKER_COUNT: usize = 32;

struct DiscoveredNode {
    address: Ipv4Addr,
    views: Vec<RemoteView>,
}

fn hosts_for_subnet(ip: Ipv4Addr, netmask: Ipv4Addr) -> Result<Vec<Ipv4Addr>, String> {
    let mask = u32::from(netmask);
    let prefix = mask.leading_ones();
    if prefix == 0 || mask != (u32::MAX << (32 - prefix)) {
        return Err(format!("Mascara de rede invalida: {}", netmask));
    }

    let total_addresses = 1u64 << (32 - prefix);
    let host_count = if prefix <= 30 {
        total_addresses - 2
    } else {
        total_addresses
    };
    if host_count > MAX_HOSTS_PER_SUBNET {
        return Err(format!(
            "Sub-rede /{} muito ampla para descoberta automatica ({} hosts).",
            prefix, host_count
        ));
    }

    let network = u32::from(ip) & mask;
    let broadcast = network | !mask;
    let first = if prefix <= 30 { network + 1 } else { network };
    let last = if prefix <= 30 {
        broadcast - 1
    } else {
        broadcast
    };
    Ok((first..=last).map(Ipv4Addr::from).collect())
}

fn active_lan_addresses() -> Result<Vec<Ipv4Addr>, String> {
    let interfaces = if_addrs::get_if_addrs()
        .map_err(|e| format!("Falha ao enumerar interfaces de rede: {}", e))?;
    let mut networks = HashSet::new();
    let mut addresses = Vec::new();
    let mut skipped_subnets = Vec::new();

    for interface in interfaces {
        let IfAddr::V4(address) = interface.addr else {
            continue;
        };
        if address.ip.is_loopback() || !address.ip.is_private() {
            continue;
        }
        let network_key = (
            u32::from(address.ip) & u32::from(address.netmask),
            address.netmask,
        );
        if !networks.insert(network_key) {
            continue;
        }
        match hosts_for_subnet(address.ip, address.netmask) {
            Ok(hosts) => addresses.extend(hosts),
            Err(error) => skipped_subnets.push(format!("{}: {}", interface.name, error)),
        }
    }

    if !skipped_subnets.is_empty() {
        eprintln!(
            "Aviso: sub-redes ignoradas por limite/mascara: {}",
            skipped_subnets.join("; ")
        );
    }
    addresses.sort_unstable();
    addresses.dedup();
    Ok(addresses)
}

fn probe_node(address: Ipv4Addr) -> Option<DiscoveredNode> {
    let socket_address = SocketAddr::from((address, PORT));
    TcpStream::connect_timeout(&socket_address, Duration::from_millis(150)).ok()?;

    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_millis(400))
        .build();
    let response = agent
        .get(&format!("http://{}:{}/catalog", address, PORT))
        .call()
        .ok()?;
    if response.status() != 200 {
        return None;
    }
    let body = response.into_string().ok()?;
    let views = serde_json::from_str::<Vec<RemoteView>>(&body).ok()?;
    Some(DiscoveredNode { address, views })
}

pub fn show_network_nodes() {
    let candidates = match active_lan_addresses() {
        Ok(candidates) => candidates,
        Err(error) => {
            println!("\x1B[1;31mErro:\x1B[0m {}", error);
            return;
        }
    };
    if candidates.is_empty() {
        println!("Nenhuma sub-rede IPv4 privada ativa encontrada.");
        return;
    }

    println!(
        "Procurando nos da engine na sub-rede local (porta {})...",
        PORT
    );
    let next_candidate = AtomicUsize::new(0);
    let nodes = Mutex::new(Vec::new());
    let candidates_ref = &candidates;
    let next_ref = &next_candidate;
    let nodes_ref = &nodes;

    std::thread::scope(|scope| {
        for _ in 0..WORKER_COUNT.min(candidates.len()) {
            scope.spawn(move || loop {
                let index = next_ref.fetch_add(1, Ordering::Relaxed);
                let Some(address) = candidates_ref.get(index).copied() else {
                    break;
                };
                if let Some(node) = probe_node(address) {
                    nodes_ref.lock().unwrap().push(node);
                }
            });
        }
    });

    let mut found = nodes.into_inner().unwrap();
    found.sort_unstable_by_key(|node| node.address);
    if found.is_empty() {
        println!("Nenhum no da Federated Engine respondeu em {}.", PORT);
        return;
    }

    println!("\nNos encontrados:");
    println!("{:<18} | {:<8} | VIEWS", "ENDERECO", "VIEWS");
    println!("{:-<18}-+-{:-<8}-+-{:-<40}", "", "", "");
    for node in found {
        let view_names = node
            .views
            .iter()
            .map(|view| format!("{}.{}", view.workspace, view.view))
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "{:<18} | {:<8} | {}",
            format!("{}:{}", node.address, PORT),
            node.views.len(),
            view_names
        );
    }
}

#[cfg(test)]
mod tests {
    use super::hosts_for_subnet;
    use std::net::Ipv4Addr;

    #[test]
    fn enumerates_only_usable_hosts_in_subnet() {
        let hosts = hosts_for_subnet(
            Ipv4Addr::new(192, 168, 1, 10),
            Ipv4Addr::new(255, 255, 255, 252),
        )
        .unwrap();
        assert_eq!(
            hosts,
            vec![
                Ipv4Addr::new(192, 168, 1, 9),
                Ipv4Addr::new(192, 168, 1, 10)
            ]
        );
    }

    #[test]
    fn rejects_oversized_subnets() {
        assert!(hosts_for_subnet(Ipv4Addr::new(10, 0, 0, 1), Ipv4Addr::new(255, 0, 0, 0)).is_err());
    }
}
