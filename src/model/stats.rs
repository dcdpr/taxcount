#[derive(Debug, Default)]
pub struct Stats {
    n_basis_rows: i32,
    n_ledger_rows: i32,
    n_trade_rows: i32,
}

impl Stats {
    pub fn inc_basis_lookup(&mut self) {
        self.n_basis_rows += 1;
    }

    pub fn inc_ledgers(&mut self) {
        self.n_ledger_rows += 1;
    }

    pub fn inc_trades(&mut self) {
        self.n_trade_rows += 1;
    }

    pub fn pretty_print(&self) {
        println!("{self:#?}");
        println!();
    }
}
