const ADAPTER_QUERY_TIMEOUT_SECONDS: u64 = 10;
const POSTGRES_STATEMENT_TIMEOUT_SECONDS: u64 = 12;
const III_INVOCATION_TIMEOUT_SECONDS: u64 = 15;

fn timeout_contract_diagnostic() -> String {
    format!(
        "adapter_query_timeout={}s postgres_role_statement_timeout={}s iii_invocation_timeout={}s",
        ADAPTER_QUERY_TIMEOUT_SECONDS,
        POSTGRES_STATEMENT_TIMEOUT_SECONDS,
        III_INVOCATION_TIMEOUT_SECONDS
    )
}

fn main() {
    println!("{}", timeout_contract_diagnostic());
}

#[cfg(test)]
mod tests {
    #[test]
    fn timeout_contract_uses_explicit_adapter_database_and_invocation_values() {
        assert_eq!(
            super::timeout_contract_diagnostic(),
            "adapter_query_timeout=10s postgres_role_statement_timeout=12s iii_invocation_timeout=15s"
        );
    }
}
