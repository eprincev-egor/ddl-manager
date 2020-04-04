create table orders (
    id integer,
    profit integer, 
    constraint orders_profit_validate 
        check( profit > 100 )
)