import { testDependencies } from "./testDependencies";

describe("CacheDependencies", () => {

    testDependencies({
        cache: `cache test for companies (
            select 
                count( orders.id_client ) as orders_count
            from public.orders as orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies as cmp (
            select 
                count( orders.id_client ) as orders_count
            from public.orders as orders
            where
                orders.id_client = cmp.id and
                orders.id_client_country = cmp.id_country
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id",
                    "id_country"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "id_client_country"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                array_agg( orders.id ) as orders_ids
            from public.orders as orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id",
                    "id_client"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                string_agg( orders.id ) as orders_ids,
                string_agg( distinct from_country.name, ', ' ) as orders_countries_names
            from public.orders as orders
            
            left join public.countries as from_country on
                from_country.id = orders.id_country

            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id",
                    "id_client",
                    "id_country"
                ]
            },
            "public.countries": {
                columns: [
                    "id",
                    "name"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( profit ) as orders_profit_sum
            from public.orders as orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( profit ) as orders_profit_sum
        )`,
        error: /required table for columnLink profit/i
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( wrong_table.profit ) as orders_profit_sum
            from public.orders as orders
            where
                orders.id_client = companies.id
        )`,
        error: /source for column wrong_table.profit not found/i
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( profit ) as orders_profit_sum
            from a, b
        )`,
        error: /required table for columnLink profit/i
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                array_agg( orders.ID ) as orders_ids
            from public.orders as orders
            where
                orders.id_CLIENT = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id",
                    "id_client"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( PROFIT ) as orders_profit_sum
            from public.orders as orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( "PROFIT" ) as orders_profit_sum
            from public.orders as orders
            where
                orders.ID_CLIENT = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "PROFIT",
                    "id_client"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( profit ) as orders_profit_sum
            from orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( public.orders.profit ) as orders_profit_sum
            from orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( profit ) as orders_profit_sum
            from ORDERS
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( ORDERS.profit ) as orders_profit_sum
            from orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( "ORDERS".profit ) as orders_profit_sum
            from "ORDERS"
            where
                "ORDERS".id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.ORDERS": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( "ORDERS".profit ) as orders_profit_sum
            from public.orders as "ORDERS"
            where
                "ORDERS".id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( public.orders.profit ) as orders_profit_sum
            from public.orders as orders
            where
                orders.id_client = companies.id
        )`,
        error: /source for column public\.orders\.profit not found/
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                sum( orderS.profit ) as orders_profit_sum
            from public.orders as Orders
            where
                orDers.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                string_agg( "Orders".profit::text || 'name', ', ' ) as "some1"
            from public.orders as "Orders"
            where
                "Orders".id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                string_agg( 'orders.name', ', ' ) as "some1"
            from orders
            where
                orders.id_client = companies.id
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                string_agg( 'orders.name', ', ' ) as "some1"
            from orders
            where
                orders.id_client = companies.id and
                coalesce(profit, null) > 0
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                    "id"
                ]
            },
            "public.orders": {
                columns: [
                    "id_client",
                    "profit"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for orders (
            select 
                count( * ) as cargos_count
            from cargos
            where
                cargos.id_order = orders.id
        )`,
        dependencies: {
            "public.orders": {
                columns: [
                    "id"
                ]
            },
            "public.cargos": {
                "columns": [
                    "id_order"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies as original_company (
            select 
                string_agg( distinct partner_company.name, ', ' ) as partners_names,
                array_agg( partner_company.name ) as partners_ids
            
            from partner_links as link
            
            inner join companies as partner_company on
                partner_company.id in ( 
                    link.id_left_company, 
                    link.id_right_company 
                ) 
                and
                partner_company.id <> original_company.id
                
            where
                original_company.id in (
                    link.id_left_company, 
                    link.id_right_company
                )
        )`,
        dependencies: {
            "public.companies": {
                columns: [
                    "id",
                    "name"
                ]
            },
            "public.partner_links": {
                "columns": [
                    "id_left_company",
                    "id_right_company"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for my_table (
            select 
                a.name || b.name || c.name || d.name as abcd
            from a
            
            left join b on 
                b.id = a.id_b
            
            right join c 
            on  c.id = b.id_c

            inner join d 
            on d.id = c.id_d

            limit 1
        )`,
        dependencies: {
            "public.my_table": {
                columns: []
            },
            "public.a": {
                "columns": [
                    "id_b",
                    "name"
                ]
            },
            "public.b": {
                "columns": [
                    "id",
                    "id_c",
                    "name"
                ]
            },
            "public.c": {
                "columns": [
                    "id",
                    "id_d",
                    "name"
                ]
            },
            "public.d": {
                "columns": [
                    "id",
                    "name"
                ]
            }
        }
    });

    testDependencies({
        cache: `cache test for companies (
            select 
                count( * ) as orders_count
            from orders
        )`,
        dependencies: {
            "public.companies": {
                "columns": [
                ]
            },
            "public.orders": {
                columns: [
                ]
            }
        }
    });

});