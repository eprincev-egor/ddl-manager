create or replace function cache000_totals_for_account_date_balance_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.balance_date is not distinct from old.balance_date then
        return new;
    end if;


    select
        sum(capital.transfer.delta) as transfers_sum
    from capital.transfer
    where
        capital.transfer.transfer_date = new.balance_date
    into new_totals;


    new.transfers_sum = new_totals.transfers_sum;


    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_account_date_balance_bef_upd
before update of balance_date
on capital.account_date_balance
for each row
execute procedure cache000_totals_for_account_date_balance_bef_upd();