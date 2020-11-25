cache totals for capital.account_date_balance as date_balance (
    select
        sum( capital.transfer.delta ) as transfers_sum
    from capital.transfer
    where
        capital.transfer.transfer_date = date_balance.balance_date
)