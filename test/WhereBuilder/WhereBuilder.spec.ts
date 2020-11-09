import { testWhereBuilder } from "./testWhereBuilder";

describe("WhereBuilder", () => {

    it("from orders where orders.id_client = companies.id", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies (
                    select count(*) as orders_count
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `,
            where: {
                "public.orders": `
                    from orders
                    where
                        orders.id_client = companies.id
                `
            }
        });
    });

    it("join one table", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select array_agg( cargo_positions.id ) as cargo_positions_ids
                    from cargo_positions
                    
                    left join cargos on
                        cargos.id = cargo_positions.id_cargo

                    where
                        cargos.id_order = orders.id
                )
            `,
            where: {
                "public.cargos": `
                    from cargos
                    where
                        cargos.id_order = orders.id
                `,
                "public.cargo_positions": `
                    from 
                        cargo_positions,
                        cargos

                    where
                        cargos.id_order = orders.id and
                        cargos.id = cargo_positions.id_cargo
                `
            }
        });
    });

    it("join one table with aliases", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders as my_order (
                    select array_agg( pos.id ) as cargo_positions_ids
                    from cargo_positions as pos
                    
                    left join cargos as cargo on
                        cargo.id = pos.id_cargo

                    where
                        cargo.id_order = my_order.id
                )
            `,
            where: {
                "public.cargos": `
                    from cargos
                    where
                        cargos.id_order = my_order.id
                `,
                "public.cargo_positions": `
                    from 
                        cargo_positions,
                        cargos as cargo

                    where
                        cargo.id_order = my_order.id and
                        cargo.id = cargo_positions.id_cargo
                `
            }
        });
    });

    it("twice join one table", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select
                        string_agg( distinct company_from.name ) as from_companies,
                        string_agg( distinct company_to.name ) as to_companies
                    from payments
                    
                    left join companies as company_from on
                        company_from.id = payments.id_company_from
                    
                    left join companies as company_to on
                        company_to.id = payments.id_company_to

                    where
                        payments.id_order = orders.id
                )
            `,
            where: {
                "public.companies": `
                    from companies, payments
                    where
                        payments.id_order = orders.id
                        and
                        companies.id = payments.id_company_from

                        or

                        payments.id_order = orders.id
                        and
                        companies.id = payments.id_company_to
                `,
                "public.payments": `
                    from payments

                    where
                        payments.id_order = orders.id
                `
            }
        });
    });

    it("where condition with operator OR", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select
                        string_agg( distinct company_from.name ) as from_companies,
                        string_agg( distinct company_to.name ) as to_companies
                    from 
                        payments, 
                        companies as company_from,
                        companies as company_to
                    
                    where
                        payments.id_order = orders.id
                        and 
                        company_to.id = payments.id_company_to

                        or

                        payments.id_order = orders.id
                        and
                        company_from.id = payments.id_company_from
                )
            `,
            where: {
                "public.companies": `
                    from companies, payments
                    where
                        payments.id_order = orders.id
                        and
                        companies.id = payments.id_company_to
                        
                        or

                        payments.id_order = orders.id
                        and
                        companies.id = payments.id_company_from
                `,
                "public.payments": `
                    from 
                        payments

                    where
                        payments.id_order = orders.id
                `
            }
        });
    });

    it("select without where", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select 
                        usd_curs as usd_curs
                    from rub_curses

                    order by date
                    limit 1
                )
            `,
            where: {
                "public.rub_curses": `
                    from rub_curses
                `
            }
        });
    });

    it("select where true", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select 
                        usd_curs as usd_curs
                    from rub_curses

                    where true

                    order by date
                    limit 1
                )
            `,
            where: {
                "public.rub_curses": `
                    from rub_curses
                `
            }
        });
    });

    it("select where false", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select 
                        usd_curs as usd_curs
                    from rub_curses

                    where false
                )
            `,
            where: {
                "public.rub_curses": `
                    from rub_curses
                    where false
                `
            }
        });
    });

    it("join on true", () => {
        testWhereBuilder({
            cache: `
                cache totals for orders (
                    select 
                        settings.base_price + client.base_price + orders.price as total_price

                    from companies as client

                    left join settings on true

                    where 
                        client.id = orders.id_client
                )
            `,
            where: {
                "public.settings": `
                    from settings
                `,
                "public.companies": `
                    from companies
                    where
                        companies.id = orders.id_client
                `
            }
        });
    });

    it("inner join after left join", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies (
                    select 
                        sum( cargos.netto ) as cargos_netto
                    from cargos

                    left join orders on
                        orders.id = cargos.id_order

                    inner join countries on 
                        countries.id = orders.id_country

                    where
                        orders.id_client = companies.id
                )
            `,
            where: {
                "public.orders": `
                    from orders
                    where
                        orders.id_client = companies.id
                `,
                "public.cargos": `
                    from cargos, orders
                    where
                        orders.id_client = companies.id
                        and
                        orders.id = cargos.id_order
                `,
                "public.countries": `
                    from countries, orders
                    where
                        orders.id_client = companies.id
                        and
                        countries.id = orders.id_country
                `
            }
        });
    });

    it("cache for companies, where settings.id = 1", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies (
                    select
                        settings.default_price as default_price
                    from settings

                    where
                        settings.id = 1
                )
            `,
            where: {
                "public.settings": `
                    from settings
                    where
                        settings.id = 1
                `
            }
        });
    });

    it("cache for companies, where settings.id = 1", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies as some_company (
                    select
                        first_company.name as first_company_name
                    from companies as first_company

                    where
                        first_company.id = 1
                )
            `,
            where: {
                "public.companies": `
                    from companies
                    where
                        companies.id = 1
                `
            }
        });
    });

    it("cache linked companies", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies as some_company (
                    select
                        string_agg( distinct linked_company.name, ', ' ) as linked_companies_names
                    from companies as linked_company

                    where
                        linked_company.id = any(some_company.linked_companies_ids) and
                        linked_company.id <> some_company.id
                )
            `,
            where: {
                "public.companies": `
                    from companies
                    where
                        companies.id = any(some_company.linked_companies_ids) and
                        companies.id <> some_company.id
                `
            }
        });
    });


    it("cache first orders", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies (
                    select
                        sum( orders.price ) as first_orders_price
                    from orders

                    where
                        orders.id in (1,2,3)
                )
            `,
            where: {
                "public.orders": `
                    from orders
                    where
                        orders.id in (1,2,3)
                `
            }
        });
    });

    it("cache companies.id in (orders.id_client, orders.id_partner)", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies (
                    select
                        sum( orders.price ) as orders_price
                    from orders

                    where
                        companies.id in (orders.id_client, orders.id_partner)
                )
            `,
            where: {
                "public.orders": `
                    from orders
                    where
                        companies.id in (orders.id_client, orders.id_partner)
                `
            }
        });
    });

    it("join on   a OR b", () => {
        testWhereBuilder({
            cache: `
                cache totals for companies as some_company (
                    select 
                        sum( orders.price ) as orders_price
                    from orders

                    inner join companies as order_company on
                        order_company.id = orders.id_client
                        or
                        order_company.id = orders.id_partner
                    
                    where
                        order_company.id = any( some_company.linked_companies )
                )
            `,
            where: {
                "public.orders": `
                    from orders, companies as order_company
                    where
                        order_company.id = any( some_company.linked_companies )
                        and
                        order_company.id = orders.id_client

                        or

                        order_company.id = any( some_company.linked_companies )
                        and
                        order_company.id = orders.id_partner
                `,
                "public.companies": `
                    from companies
                    where
                        companies.id = any( some_company.linked_companies )
                `
            }
        });
    });
});