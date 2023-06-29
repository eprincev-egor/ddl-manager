create or replace function cache000_totals_for_account_date_balance_bef_ins()
returns trigger as $body$
declare new_totals record;
begin



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

create trigger cache000_totals_for_account_date_balance_bef_ins
before insert
on capital.account_date_balance
for each row
execute procedure cache000_totals_for_account_date_balance_bef_ins();