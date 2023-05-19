create or replace function cache001_log_oper_sale_vat_bef_ins()
returns trigger as $body$
begin
    new.sale_vat = calc_vat(
        new.sale_vat_type,
        new.sale_vat_value
    );

    return new;
end
$body$
language plpgsql;

create trigger cache001_log_oper_sale_vat_bef_ins
before insert
on public.log_oper
for each row
execute procedure cache001_log_oper_sale_vat_bef_ins();