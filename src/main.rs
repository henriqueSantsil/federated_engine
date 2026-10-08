mod catalog;
mod connectors;
mod exporters;
mod network_discovery;
mod parser;
mod physical_plan;
mod server;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::sync::{Arc, Mutex};

fn main() {
    print!("\x1B[2J\x1B[1;1H");
    println!("==================================================");
    println!(" Federated Data Engine - P2P Marketplace");
    println!("==================================================");

    let catalog_instance = catalog::Catalog::load();
    let catalog_arc = Arc::new(Mutex::new(catalog_instance));

    let mut rl = DefaultEditor::new().unwrap();
    let _ = rl.load_history(".engine_history");

    loop {
        let active_ws = { catalog_arc.lock().unwrap().active_workspace.clone() };
        let prompt = format!("\x1B[1;36m{}\x1B[0m> ", active_ws);

        match rl.readline(&prompt) {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(input);

                if input.eq_ignore_ascii_case("exit") || input.eq_ignore_ascii_case("quit") {
                    break;
                }

                if input.to_uppercase().starts_with("SERVE ON ") {
                    let parts: Vec<&str> = input.split_whitespace().collect();
                    if parts.len() == 3 {
                        if let Ok(port) = parts[2].trim_end_matches(';').parse::<u16>() {
                            server::start_server(catalog_arc.clone(), port);
                        } else {
                            println!("\x1B[1;31mErro:\x1B[0m Porta invalida.");
                        }
                    } else {
                        println!(
                            "\x1B[1;31mErro:\x1B[0m Sintaxe incorreta. Use: SERVE ON <porta>;"
                        );
                    }
                    continue;
                }

                let mut cat = catalog_arc.lock().unwrap();
                parser::parse_command(input, &mut cat);
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Erro no terminal: {:?}", err);
                break;
            }
        }
    }
    let _ = rl.save_history(".engine_history");
}
