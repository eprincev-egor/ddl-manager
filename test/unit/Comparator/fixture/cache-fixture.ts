import { FileParser } from "../../../../lib/parser";

export const someFileParams = {
    name: "some_file.sql",
    folder: "/",
    path: "/some_file.sql"
};

export const testCache = FileParser.parseCache(`
    cache totals for companies (
        select
            sum(orders.profit) as orders_profit
        from orders
        where
            orders.id_client = companies.id
    )
`);

export const testFileWithCache = {
    ...someFileParams,
    content: {
        cache: [testCache]
    }
};