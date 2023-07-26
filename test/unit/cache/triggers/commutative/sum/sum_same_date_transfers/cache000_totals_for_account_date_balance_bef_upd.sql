create or replace function cache000_totals_for_account_date_balance_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.balance_date is not distinct from old.balance_date then
        return new;
    end if;


    select
            sum(capital.transfer.delta) as transfers_sum,
            ('{' || string_agg(
                                        '"' || capital.transfer.id::text || '":' || jsonb_build_object(
                            'delta', capital.transfer.delta,'id', capital.transfer.id,'transfer_date', capital.transfer.transfer_date
                        )::text,
                                        ','
                                    ) || '}')
            ::
            jsonb as __totals_json__
    from capital.transfer
    where
        capital.transfer.transfer_date = new.balance_date
    into new_totals;


    new.transfers_sum = new_totals.transfers_sum;
    new.__totals_json__ = new_totals.__totals_json__;


    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_account_date_balance_bef_upd
before update of balance_date
on capital.account_date_balance
for each row
execute procedure cache000_totals_for_account_date_balance_bef_upd();