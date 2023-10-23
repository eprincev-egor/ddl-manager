import fs from "fs";
import { escapeRegExp, flatMap } from "lodash";
import { FileReader } from "../../../lib/fs/FileReader";
import {expect, use} from "chai";
import chaiShallowDeepEqualPlugin from "chai-shallow-deep-equal";
import { prepare } from "../utils/prepare";
import { CacheIndex } from "../../../lib/ast/CacheIndex";
import assert from "assert";

use(chaiShallowDeepEqualPlugin);

describe("integration/FileReader parse cache", () => {

    const ROOT_TMP_PATH = __dirname + "/tmp";
    prepare(ROOT_TMP_PATH);

    it("parse file with simple cache", () => {
        fs.writeFileSync(ROOT_TMP_PATH + "/test-file.sql", `
            cache totals for companies (
                select
                    sum( orders.profit ) as orders_profit
                from orders
                where
                    orders.id_client = companies.id
            )
        `);

        const state = FileReader.read([ROOT_TMP_PATH]);

        expect(flatMap(state.allNotHelpersFiles(), file => file.content.cache)).to.be.shallowDeepEqual([
            {
                name: "totals",
                for: {
                    table: {
                        schema: "public",
                        name: "companies"
                    },
                    alias: undefined
                },
                select: {
                    columns: [
                        {
                            expression: {elements: [
                                {name: "sum", args: [
                                    {elements: [
                                        {tableReference: {
                                            table: {
                                                schema: "public",
                                                name: "orders"
                                            },
                                            alias: undefined
                                        }, name: "profit"}
                                    ]}
                                ]}
                            ]},
                            name: "orders_profit"
                        }
                    ],
                    from: [{
                        source: {
                            table: {
                                schema: "public",
                                name: "orders"
                            },
                            alias: undefined
                        },
                        joins: []
                    }],
                    where: {elements: [
                        {tableReference: {
                            table: {
                                schema: "public",
                                name: "orders"
                            },
                            alias: undefined
                        }, name: "id_client"},
                        {operator: "="},
                        {tableReference: {
                            table: {
                                schema: "public",
                                name: "companies"
                            },
                            alias: undefined
                        }, name: "id"}
                    ]}
                }
            }
        ]);
    });

    it("parsing without errors two files with cache", () => {
        fs.writeFileSync(ROOT_TMP_PATH + "/file-1.sql", `
            cache totals1 for companies (
                select
                    sum( orders.debet ) as my_column1
                from orders
                where
                    orders.id_client = companies.id
            )
        `);
        fs.writeFileSync(ROOT_TMP_PATH + "/file-2.sql", `
            cache totals2 for companies (
                select
                    sum( orders.credit ) as my_column2
                from orders
                where
                    orders.id_client = companies.id
            )
        `);

        const state = FileReader.read([ROOT_TMP_PATH]);
        assert.strictEqual(state.allNotHelpersFiles().length, 2);
    });

    it("parse cache with index", () => {
        fs.writeFileSync(ROOT_TMP_PATH + "/file.sql", `
            cache totals for companies (
                select
                    max( orders.id ) as last_order_id
                from orders
                where
                    orders.id_client = companies.id
            )
            index btree on (last_order_id)
            index btree on (( last_order_id + 1 ))
        `);

        const state = FileReader.read([ROOT_TMP_PATH]);
        const actualCache = state.allNotHelpersFiles()[0].content.cache[0];

        assert.strictEqual(actualCache.indexes.length, 2, "should be two indexes");
        assert.ok( actualCache.indexes[0] instanceof CacheIndex, "first index instance is correct" );
        assert.ok( actualCache.indexes[1] instanceof CacheIndex, "second index instance is correct" );

        assert.deepStrictEqual(
            actualCache.indexes[0].on,
            ["last_order_id"],
            "first index.on is [string]"
        );
        assert.strictEqual(
            actualCache.indexes[1].on.toString(),
            "(last_order_id + 1)",
            "second index.on is [expression]"
        );
    });

    describe("validate", () => {
        const filePath = ROOT_TMP_PATH + "/test-file.sql";

        describe("disallow SeqScan, need use: && instead of =any()", () => {

            it("from table_name", () => {
                fs.writeFileSync(filePath, `
                    cache totals for companies (
                        select
                            sum( orders.profit ) as orders_profit
                        from orders
                        where
                            companies.id = any( orders.clients_ids )
                    )
                `);
        
                throws([
                    "your condition is slow SeqScan, condition should be:",
                    "orders.clients_ids && ARRAY[ companies.id ]"
                ].join("\n"));
            });

            it("from table_name as alias", () => {
                fs.writeFileSync(filePath, `
                    cache totals for companies (
                        select
                            sum( my_table.profit ) as orders_profit
                        from orders as my_table
                        where
                            companies.id = any( my_table.clients_ids )
                    )
                `);
        
                throws([
                    "your condition is slow SeqScan, condition should be:",
                    "my_table.clients_ids && ARRAY[ companies.id ]"
                ].join("\n"));
            });

            it("from schema_name.table_name", () => {
                fs.writeFileSync(filePath, `
                    cache totals for companies (
                        select
                            sum( orders.profit ) as orders_profit
                        from public.orders
                        where
                            companies.id = any( orders.clients_ids )
                    )
                `);
        
                throws([
                    "your condition is slow SeqScan, condition should be:",
                    "orders.clients_ids && ARRAY[ companies.id ]"
                ].join("\n"));
            });

        });

        it("required alias for columns", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( orders.profit )
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);

            throws("required alias for every cache column");
        });
    
        it("duplicated cache column name inside one file", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( orders.debet ) as orders_profit,
                        sum( orders.credit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);

            throws("duplicated cache column");
        });
    
        it("duplicated cache column name inside two files", () => {
            fs.writeFileSync(ROOT_TMP_PATH + "/file1.sql", `
                cache totals1 for companies (
                    select
                        sum( orders.debet ) as my_column
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/file2.sql", `
                cache totals2 for companies (
                    select
                        sum( orders.credit ) as my_column
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
    
            throws("duplicated columns: my_column by cache: totals2, totals1");
        });

        it("many from items is not allowed", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( a.x + b.y ) as col
                    from a, b
                )
            `);

            throws("multiple FROM are not supported");
        });

        it("supported only from table (from sub query)", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( a.x ) as col
                    from (select) as a
                )
            `);

            throws("supported only from table");
        });

        it("supported only from table (from function)", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( a.x ) as col
                    from unnest(array[1]::integer[]) as a
                )
            `);

            throws("supported only from table");
        });
    

        it("supported only from table (from values)", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( a.x ) as col
                    from unnest(values (1)) as a
                )
            `);

            throws("supported only from table");
        });
    
        it("cache name for table should be unique", () => {
            fs.writeFileSync(ROOT_TMP_PATH + "/file-1.sql", `
                cache totals for companies (
                    select
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/file-2.sql", `
                cache totals for companies (
                    select
                        sum( invoices.profit ) as invoices_profit
                    from invoices
                    where
                        invoices.id_payer = companies.id
                )
            `);

            throws("duplicated cache totals for companies");
        });

        it("sub queries are not supported (select (sub query))", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum(
                            orders.profit + 
                            (select id from order_type limit 1) 
                        ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
    
            throws("SUB QUERIES are not supported");
        });
    
        it("GROUP BY are not supported", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                    group by orders.id_client
                )
            `);
    
            throws("GROUP BY are not supported");
        });
    
        it("syntax error on string_agg without delimiter", () => {
            fs.writeFileSync(filePath, `
                cache totals for orders (
                    select
                        string_agg( invoice.doc_number ) || 'test' as invoices_numbers
                    from invoice
                    where
                        invoice.id_order = orders.id
                )
            `);
    
            throws("required delimiter for string_agg");
        });
    
        it("CTE are not supported", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    with totals as (select 1)
                    select
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
    
            throws("CTE (with queries) are not supported");
        });
    
    
        it("UNION are not supported", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                        sum( orders.profit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                    union
                    select 1
                )
            `);
    
            throws("UNION are not supported");
        });
    
        it("duplicated cache column name inside one files", () => {
            fs.writeFileSync(ROOT_TMP_PATH + "/file-1.sql", `
                cache totals1 for companies (
                    select
                        sum( orders.debet ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
            fs.writeFileSync(ROOT_TMP_PATH + "/file-2.sql", `
                cache totals2 for companies (
                    select
                        sum( orders.credit ) as orders_profit
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
    
            throws("duplicated columns: orders_profit by cache: totals2, totals1");
        });
    
        it("required cache columns", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select
                    from orders
                    where
                        orders.id_client = companies.id
                )
            `);
    
            throws("required select any columns or expressions");
        });
    
        it("no joins in order by/limit 1", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select orders.date as order_date
                    from orders

                    left join order_type on
                        order_type.id = orders.id_type

                    order by order_type.code
                    limit 1
                )
            `);
    
            throws("joins is not supported for order by/limit 1 trigger");
        });
    
        it("join should have ON condition", () => {
            fs.writeFileSync(filePath, `
                cache totals for companies (
                    select orders.date as order_date
                    from orders

                    left join order_type using (id)
                )
            `);
    
            throws("required ON condition for join");
        });
    
        it("need from item for order by / limit ", () => {
            fs.writeFileSync(filePath, `
                cache totals for orders (
                    select orders.date as order_date
                    order by orders.code
                    limit 1
                )
            `);
    
            throws("required FROM ITEM");
        });
    
        it("required limit 1", () => {
            fs.writeFileSync(filePath, `
                cache totals for orders (
                    select orders.date as order_date
                    from orders
                    order by orders.code
                )
            `);
    
            throws("required LIMIT 1");
        });
    
        it("invalid limit", () => {
            fs.writeFileSync(filePath, `
                cache totals for orders (
                    select orders.date as order_date
                    from orders
                    order by orders.code
                    limit 100
                )
            `);
    
            throws("supported only limit 1");
        });
    
        it("required order by", () => {
            fs.writeFileSync(filePath, `
                cache totals for orders (
                    select orders.date as order_date
                    from orders
                    limit 1
                )
            `);
    
            throws("required ORDER BY");
        });

    });
    
    function throws(errorText: string) {
        const errorPattern = new RegExp(escapeRegExp(errorText), "i");

        assert.throws(() => {
            FileReader.read([ROOT_TMP_PATH]);
        }, errorPattern);
    }

});