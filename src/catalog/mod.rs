use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualTable {
    pub name: String,
    pub format: String,
    pub physical_path: String,
    #[serde(default)]
    pub source_url: Option<String>,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub tables: HashMap<String, VirtualTable>,
    pub views: HashMap<String, String>,
    #[serde(default)]
    pub published_views: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub active_workspace: String,
    pub workspaces: HashMap<String, Workspace>,
}

impl Catalog {
    pub fn new() -> Self {
        let mut workspaces = HashMap::new();
        workspaces.insert(
            "default".to_string(),
            Workspace {
                tables: HashMap::new(),
                views: HashMap::new(),
                published_views: Vec::new(),
            },
        );
        Self {
            active_workspace: "default".to_string(),
            workspaces,
        }
    }

    pub fn load() -> Self {
        if let Ok(content) = fs::read_to_string(".federated_catalog.yaml") {
            if let Ok(catalog) = serde_yaml::from_str(&content) {
                return catalog;
            }
        }
        let catalog = Self::new();
        catalog.save();
        catalog
    }

    pub fn save(&self) {
        if let Ok(content) = serde_yaml::to_string(self) {
            let _ = fs::write(".federated_catalog.yaml", content);
        } else {
            println!("\x1B[1;31mErro:\x1B[0m Falha ao salvar o catalogo no YAML.");
        }
    }

    pub fn add_table(&mut self, table: VirtualTable) -> Result<(), String> {
        if let Some(workspace) = self.workspaces.get_mut(&self.active_workspace) {
            workspace.tables.insert(table.name.clone(), table);
            self.save();
            Ok(())
        } else {
            Err(format!(
                "Workspace '{}' nao encontrado.",
                self.active_workspace
            ))
        }
    }

    pub fn replace_table_qualified(
        &mut self,
        full_name: &str,
        fallback_ws: &str,
        table: VirtualTable,
    ) -> Result<(), String> {
        let (workspace_name, table_name) =
            if let Some((workspace, name)) = full_name.split_once('.') {
                (workspace, name)
            } else {
                (fallback_ws, full_name)
            };
        let workspace = self
            .workspaces
            .get_mut(workspace_name)
            .ok_or_else(|| format!("Workspace '{}' nao encontrado.", workspace_name))?;
        if !workspace.tables.contains_key(table_name) {
            return Err(format!("Tabela '{}' nao encontrada.", full_name));
        }
        workspace.tables.insert(table_name.to_string(), table);
        self.save();
        Ok(())
    }

    pub fn create_workspace(&mut self, name: &str) {
        if self.workspaces.contains_key(name) {
            println!("\x1B[1;33mAviso:\x1B[0m O workspace '{}' ja existe.", name);
        } else {
            self.workspaces.insert(
                name.to_string(),
                Workspace {
                    tables: HashMap::new(),
                    views: HashMap::new(),
                    published_views: Vec::new(),
                },
            );
            self.save();
            println!("\x1B[1;32mSucesso:\x1B[0m Workspace '{}' criado.", name);
        }
    }

    pub fn use_workspace(&mut self, name: &str) {
        if self.workspaces.contains_key(name) {
            self.active_workspace = name.to_string();
            self.save();
            println!("Workspace alterado para: \x1B[1;36m{}\x1B[0m", name);
        } else {
            println!(
                "\x1B[1;31mErro:\x1B[0m Workspace '{}' nao encontrado.",
                name
            );
        }
    }

    pub fn show_workspaces(&self) {
        println!("\nWorkspaces Disponiveis:");
        for name in self.workspaces.keys() {
            if name == &self.active_workspace {
                println!("  * \x1B[1;36m{}\x1B[0m (ativo)", name);
            } else {
                println!("    {}", name);
            }
        }
        println!();
    }

    pub fn show_tables(&self) {
        if let Some(workspace) = self.workspaces.get(&self.active_workspace) {
            println!(
                "\nTabelas e Views no workspace '\x1B[1;36m{}\x1B[0m':",
                self.active_workspace
            );
            let has_tables = !workspace.tables.is_empty();
            let has_views = !workspace.views.is_empty();

            if !has_tables && !has_views {
                println!("  (Nenhuma tabela ou view cadastrada)");
            } else {
                if has_tables {
                    println!(
                        "\n[ TABELAS FISICAS ]\n{:<20} | {:<10} | {:<30} | {}",
                        "NOME", "FORMATO", "CAMINHO FISICO", "TAMANHO"
                    );
                    println!("{:-<20}-+-{:-<10}-+-{:-<30}-+-{:-<15}", "", "", "", "");
                    for (name, table) in &workspace.tables {
                        let size_str = match std::fs::metadata(&table.physical_path) {
                            Ok(meta) => format!("{:.2} KB", meta.len() as f64 / 1024.0),
                            Err(_) if table.source_url.is_some() => "[ SEM CACHE ]".to_string(),
                            Err(_) => "[ INACESSIVEL ]".to_string(),
                        };
                        println!(
                            "{:<20} | {:<10} | {:<30} | {}",
                            name, table.format, table.physical_path, size_str
                        );
                    }
                }
                if has_views {
                    println!(
                        "\n[ VIEWS (QUERYS SALVAS) ]\n{:<20} | {:<20} | {}",
                        "NOME DA VIEW", "STATUS (REDE)", "QUERY ASSOCIADA"
                    );
                    println!("{:-<20}-+-{:-<20}-+-{:-<50}", "", "", "");
                    for (name, query) in &workspace.views {
                        let status = if workspace.published_views.contains(name) {
                            "\x1B[1;32m[ PUBLICADA ]\x1B[0m"
                        } else {
                            "[ LOCAL ]"
                        };
                        println!("{:<20} | {:<30} | {}", name, status, query);
                    }
                }
            }
            println!();
        }
    }

    pub fn describe_table(&self, full_name: &str) {
        let (ws_name, obj_name) = if full_name.contains('.') {
            let parts: Vec<&str> = full_name.split('.').collect();
            if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (self.active_workspace.as_str(), full_name)
            }
        } else {
            (self.active_workspace.as_str(), full_name)
        };
        if let Some(workspace) = self.workspaces.get(ws_name) {
            if let Some(table) = workspace.tables.get(obj_name) {
                println!(
                    "\nEsquema da tabela '{}':\n{:<30} | {}",
                    obj_name, "COLUNA", "TIPO DE DADO"
                );
                println!("{:-<30}-+-{:-<20}", "", "");
                for col in &table.columns {
                    println!("{:<30} | {}", col.name, col.data_type);
                }
                println!();
                return;
            }
            if let Some(query) = workspace.views.get(obj_name) {
                println!(
                    "\nEsquema da view '{}':\n{:<30} | {}",
                    obj_name, "COLUNA", "TIPO DE DADO (Herdado)"
                );
                println!("{:-<30}-+-{:-<20}", "", "");
                let dialect = GenericDialect {};
                if let Ok(ast) = Parser::parse_sql(&dialect, query) {
                    if let Some(Statement::Query(q)) = ast.first() {
                        if let sqlparser::ast::SetExpr::Select(select) = &*q.body {
                            if let Some(table_with_joins) = select.from.first() {
                                if let sqlparser::ast::TableFactor::Table { name, .. } =
                                    &table_with_joins.relation
                                {
                                    let base_table_name = name
                                        .0
                                        .iter()
                                        .map(|i| i.value.clone())
                                        .collect::<Vec<_>>()
                                        .join(".");
                                    if let Some(base_table) =
                                        self.get_table_qualified(&base_table_name, ws_name)
                                    {
                                        let mut proj_cols = Vec::new();
                                        for item in &select.projection {
                                            match item {
                                                sqlparser::ast::SelectItem::UnnamedExpr(
                                                    sqlparser::ast::Expr::Identifier(ident),
                                                ) => {
                                                    proj_cols.push(ident.value.clone());
                                                }
                                                sqlparser::ast::SelectItem::Wildcard(_) => {
                                                    proj_cols.clear();
                                                    break;
                                                }
                                                _ => {}
                                            }
                                        }
                                        if proj_cols.is_empty() {
                                            for col in &base_table.columns {
                                                println!("{:<30} | {}", col.name, col.data_type);
                                            }
                                        } else {
                                            for col_name in proj_cols {
                                                let mut found_type = "Desconhecido".to_string();
                                                for base_col in &base_table.columns {
                                                    if base_col.name == col_name {
                                                        found_type = base_col.data_type.clone();
                                                        break;
                                                    }
                                                }
                                                println!("{:<30} | {}", col_name, found_type);
                                            }
                                        }
                                        println!();
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                println!("\x1B[1;33m(Aviso: Nao foi possivel inferir o esquema automaticamente)\x1B[0m\n");
                return;
            }
            println!(
                "\x1B[1;31mErro:\x1B[0m Tabela ou View '{}' nao encontrada.",
                obj_name
            );
        }
    }

    pub fn drop_table(&mut self, name: &str) {
        if let Some(workspace) = self.workspaces.get_mut(&self.active_workspace) {
            if workspace.tables.remove(name).is_some() {
                self.save();
                println!("\x1B[1;32mSucesso:\x1B[0m Tabela '{}' removida.", name);
            } else {
                println!("\x1B[1;31mErro:\x1B[0m Tabela '{}' nao encontrada.", name);
            }
        }
    }

    pub fn alter_column_type(&mut self, table_name: &str, column_name: &str, new_type: &str) {
        if let Some(workspace) = self.workspaces.get_mut(&self.active_workspace) {
            if let Some(table) = workspace.tables.get_mut(table_name) {
                let mut found = false;
                for col in &mut table.columns {
                    if col.name.eq_ignore_ascii_case(column_name) {
                        col.data_type = new_type.to_string();
                        found = true;
                        break;
                    }
                }
                if found {
                    self.save();
                    println!("\x1B[1;32mSucesso:\x1B[0m Tipo alterado.");
                } else {
                    println!("\x1B[1;31mErro:\x1B[0m Coluna nao encontrada.");
                }
            }
        }
    }

    pub fn get_table_qualified(&self, full_name: &str, fallback_ws: &str) -> Option<&VirtualTable> {
        if full_name.contains('.') {
            let parts: Vec<&str> = full_name.split('.').collect();
            if parts.len() == 2 {
                return self.workspaces.get(parts[0])?.tables.get(parts[1]);
            }
        }
        self.workspaces.get(fallback_ws)?.tables.get(full_name)
    }

    pub fn add_view(&mut self, view_name: String, query_sql: String) {
        if let Some(workspace) = self.workspaces.get_mut(&self.active_workspace) {
            workspace.views.insert(view_name.clone(), query_sql);
            self.save();
            println!("\x1B[1;32mSucesso:\x1B[0m View '{}' criada.", view_name);
        }
    }

    pub fn publish_view(&mut self, view_name: &str) {
        if let Some(workspace) = self.workspaces.get_mut(&self.active_workspace) {
            if workspace.views.contains_key(view_name) {
                if !workspace.published_views.contains(&view_name.to_string()) {
                    workspace.published_views.push(view_name.to_string());
                    self.save();
                    println!("\x1B[1;32mSucesso:\x1B[0m View etiquetada para o Marketplace!");
                } else {
                    println!("\x1B[1;33mAviso:\x1B[0m A view ja esta publicada.");
                }
            } else {
                println!("\x1B[1;31mErro:\x1B[0m View nao encontrada.");
            }
        }
    }

    pub fn get_view_qualified(
        &self,
        full_name: &str,
        fallback_ws: &str,
    ) -> Option<(String, String)> {
        let (ws_name, view_name) = if full_name.contains('.') {
            let parts: Vec<&str> = full_name.split('.').collect();
            if parts.len() == 2 {
                (parts[0], parts[1])
            } else {
                (fallback_ws, full_name)
            }
        } else {
            (fallback_ws, full_name)
        };
        let query = self.workspaces.get(ws_name)?.views.get(view_name)?;
        Some((ws_name.to_string(), query.clone()))
    }

    pub fn get_view_columns_qualified(
        &self,
        full_name: &str,
        fallback_ws: &str,
    ) -> Option<Vec<ColumnDef>> {
        let (view_workspace, view_name) = if let Some((workspace, name)) = full_name.split_once('.')
        {
            (workspace, name)
        } else {
            (fallback_ws, full_name)
        };
        let view_query = self.workspaces.get(view_workspace)?.views.get(view_name)?;
        let statements = Parser::parse_sql(&GenericDialect {}, view_query).ok()?;
        let Statement::Query(query) = statements.first()? else {
            return None;
        };
        let sqlparser::ast::SetExpr::Select(select) = &*query.body else {
            return None;
        };
        let table_with_joins = select.from.first()?;
        if !table_with_joins.joins.is_empty() {
            return None;
        }
        let sqlparser::ast::TableFactor::Table { name, .. } = &table_with_joins.relation else {
            return None;
        };
        let base_name = name
            .0
            .iter()
            .map(|ident| ident.value.clone())
            .collect::<Vec<_>>()
            .join(".");
        let base_table = self.get_table_qualified(&base_name, view_workspace)?;

        let mut columns = Vec::new();
        for item in &select.projection {
            let (column_name, alias) = match item {
                sqlparser::ast::SelectItem::Wildcard(_) => return Some(base_table.columns.clone()),
                sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(
                    ident,
                )) => (ident.value.as_str(), None),
                sqlparser::ast::SelectItem::UnnamedExpr(
                    sqlparser::ast::Expr::CompoundIdentifier(identifiers),
                ) => (identifiers.last()?.value.as_str(), None),
                sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::Identifier(ident),
                    alias,
                } => (ident.value.as_str(), Some(alias.value.as_str())),
                sqlparser::ast::SelectItem::ExprWithAlias {
                    expr: sqlparser::ast::Expr::CompoundIdentifier(identifiers),
                    alias,
                } => (
                    identifiers.last()?.value.as_str(),
                    Some(alias.value.as_str()),
                ),
                _ => return None,
            };
            let mut column = base_table
                .columns
                .iter()
                .find(|column| column.name == column_name)?
                .clone();
            if let Some(alias) = alias {
                column.name = alias.to_string();
            }
            columns.push(column);
        }
        Some(columns)
    }

    pub fn drop_view(&mut self, name: &str) {
        if let Some(workspace) = self.workspaces.get_mut(&self.active_workspace) {
            if workspace.views.remove(name).is_some() {
                workspace.published_views.retain(|v| v != name);
                self.save();
                println!("\x1B[1;32mSucesso:\x1B[0m View removida.");
            } else {
                println!("\x1B[1;31mErro:\x1B[0m View nao encontrada.");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Catalog, ColumnDef, VirtualTable};

    #[test]
    fn legacy_catalogs_deserialize_without_source_url() {
        let yaml = r#"
active_workspace: default
workspaces:
  default:
    tables:
      climate:
        name: climate
        format: CSV
        physical_path: .cache_climate.csv
        columns:
          - name: year
            data_type: Int64
    views: {}
    published_views: []
"#;
        let catalog: Catalog = serde_yaml::from_str(yaml).unwrap();
        assert!(catalog
            .get_table_qualified("climate", "default")
            .unwrap()
            .source_url
            .is_none());
    }

    #[test]
    fn resolves_simple_view_schema_and_aliases() {
        let mut catalog = Catalog::new();
        let workspace = catalog.workspaces.get_mut("default").unwrap();
        workspace.tables.insert(
            "climate".to_string(),
            VirtualTable {
                name: "climate".to_string(),
                format: "CSV".to_string(),
                physical_path: "unused.csv".to_string(),
                source_url: None,
                columns: vec![
                    ColumnDef {
                        name: "year".to_string(),
                        data_type: "Int64".to_string(),
                    },
                    ColumnDef {
                        name: "place".to_string(),
                        data_type: "Utf8".to_string(),
                    },
                ],
            },
        );
        workspace.views.insert(
            "climate_view".to_string(),
            "SELECT year AS harvest_year, place FROM climate WHERE year > 2020".to_string(),
        );

        let columns = catalog
            .get_view_columns_qualified("climate_view", "default")
            .unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["harvest_year", "place"]
        );
        assert_eq!(columns[0].data_type, "Int64");
    }
}
