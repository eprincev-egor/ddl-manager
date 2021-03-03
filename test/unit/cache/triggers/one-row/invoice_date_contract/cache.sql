cache contract for invoice (
    select
        date_contract as date_contract
    from list_contracts
    where
        list_contracts.id = invoice.id_list_contracts
)