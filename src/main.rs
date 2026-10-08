mod catalog;
mod cli;
mod connectors;
mod exporters;
mod network_discovery;
mod parser;
mod physical_plan;
mod server;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq, Eq)]
enum InterfaceMode {
    Raw,
    Selectable,
}

fn main() {
    print!("\x1B[2J\x1B[1;1H");
    println!("==================================================");
    println!(" Federated Data Engine - P2P Marketplace");
    println!("==================================================");

    let catalog_instance = catalog::Catalog::load();
    let catalog_arc = Arc::new(Mutex::new(catalog_instance));

    let mut rl = DefaultEditor::new().unwrap();
    let _ = rl.load_history(".engine_history");

    let mut mode = match cli::choose_interface_mode() {
        Ok(mode) => mode,
        Err(error) => {
            println!("\x1B[1;31mErro na interface:\x1B[0m {}", error);
            return;
        }
    };

    loop {
        match mode {
            InterfaceMode::Raw => match run_raw_mode(&mut rl, catalog_arc.clone()) {
                Some(next_mode) => mode = next_mode,
                None => break,
            },
            InterfaceMode::Selectable => match cli::run_selectable_mode(catalog_arc.clone()) {
                Ok(Some(next_mode)) => mode = next_mode,
                Ok(None) => break,
                Err(error) => {
                    println!("\x1B[1;31mErro na interface guiada:\x1B[0m {}", error);
                    mode = InterfaceMode::Raw;
                }
            },
        }
    }
    let _ = rl.save_history(".engine_history");
}

fn run_raw_mode(
    rl: &mut DefaultEditor,
    catalog_arc: Arc<Mutex<catalog::Catalog>>,
) -> Option<InterfaceMode> {
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
                    return None;
                }
                if input.trim_end_matches(';').eq_ignore_ascii_case("MODE TUI") {
                    return Some(InterfaceMode::Selectable);
                }
                if input.trim_end_matches(';').eq_ignore_ascii_case("MODE RAW") {
                    println!("A interface ja esta no modo raw.");
                    continue;
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
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => return None,
            Err(err) => {
                println!("Erro no terminal: {:?}", err);
                return None;
            }
        }
    }
}
