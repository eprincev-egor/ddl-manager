import { Database } from "../../../../lib/database/schema/Database";
import { Table } from "../../../../lib/database/schema/Table";
import { Column } from "../../../../lib/database/schema/Column";
import { TableID } from "../../../../lib/database/schema/TableID";

export const testDatabase = new Database([
    table("companies", {
        bigint_orders_ids: "bigint[]",
        total_profit: "numeric"
    }),
    table("orders", {
        doc_number: "text",
        companies_ids: "integer[]",
        clients_ids: "integer[]",
        partners_ids: "integer[]",
        order_date: "date",
        date: "date",
        profit: "numeric",
    }),
    table("order", {
        some_date: "date"
    }),
    table("vats", {vat_value: "numeric"}),
    table("unit_type", {
        id_category: "integer",
        name: "Text"
    }),
    table("invoice", {
        renomination_invoices: "int8[]",
        payments_ids: "int8[]",
        orders_ids: "bigint[]",
        invoice_summ: "numeric"
    }),
    table("invoice_positions", {
        id_invoice: "integer"
    }),
    table("train", { units_ids: "int8[]" }),
    table("train_unit_link", {
        id_train: "integer"
    }),
    table("list_gtd", {
        orders_ids: "bigint[]",
        operation_units_ids: "bigint[]",
        date_clear: "date"
    }),
    table("operation.operation", {
        id_operation_type: "integer",
        doc_parent_id_order: "bigint",
        id_order: "bigint",
        units_ids: "bigint[]",
        end_expected_date: "date",
        cost_sale: "numeric",
        deleted: "smallint"
    }),
    table("operations", {
        units_ids: "bigint[]",
        incoming_date: "date",
        outgoing_date: "date",
        doc_number: "text"
    }),
    table("arrival_points", {
        id_point: "integer",
        actual_date: "date",
        expected_date: "date",
        sort: "integer"
    }),
    table("user_task", {
        query_name: "text",
        row_id: "int8"
    }),
    table("tasks", {
        watchers_ids: "text",
        orders_managers_ids: "text"
    }),
    table("user_task_watcher_link", {
        id_user: "integer"
    }),
    table("units", { orders_ids: "bigint[]" }),
    table("log_oper", {
        orders_ids: "bigint[]",
        buy_vat_type: "integer",
        buy_vat_value: "numeric",
        sale_vat_type: "integer",
        sale_vat_value: "numeric"
    }),
    table("rates", {
        price: "numeric",
        quantity: "integer",
        vat_type: "integer",
        vat_value: "numeric"
    }),
    table("countries", {
        has_surveyor_inspection: "boolean"
    }),
    table("comments", {
        message: "text"
    }),
    table("list_contracts", {
        date_contract: "date"
    })
]);

function table(schemaName: string, columns: Record<string, string>) {
    const tableID = TableID.fromString(schemaName);
    return new Table(
        tableID.schema,
        tableID.name,
        Object.entries(columns).map(([key, type]) => new Column(
            tableID,
            key, type
        ))
    );
}