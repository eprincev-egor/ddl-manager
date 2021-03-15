import { Database } from "../../../../lib/database/schema/Database";
import { Table } from "../../../../lib/database/schema/Table";
import { Column } from "../../../../lib/database/schema/Column";
import { TableID } from "../../../../lib/database/schema/TableID";

const companiesID = new TableID(
    "public",
    "companies",
);
const ordersID = new TableID(
    "public",
    "orders",
);
const vatsID = new TableID(
    "public",
    "vats",
);
const unitTypesID = new TableID(
    "public",
    "unit_type",
);
const invoiceID = new TableID(
    "public",
    "invoice",
);
const trainID = new TableID(
    "public",
    "train",
);
const gtdID = new TableID(
    "public",
    "list_gtd",
);
const operationID = new TableID(
    "operation",
    "operation",
);
const operationsID = new TableID(
    "public",
    "operations",
);
const userTaskID = new TableID(
    "public",
    "user_task",
);

export const testDatabase = new Database([
    new Table(
        companiesID.schema,
        companiesID.name,
        [
            new Column(
                companiesID,
                "bigint_orders_ids",
                "bigint[]"
            )
        ]
    ),
    new Table(
        ordersID.schema,
        ordersID.name,
        [
            new Column(
                ordersID,
                "companies_ids",
                "integer[]"
            ),
            new Column(
                ordersID,
                "clients_ids",
                "integer[]"
            ),
            new Column(
                ordersID,
                "partners_ids",
                "integer[]"
            ),
            new Column(
                ordersID,
                "order_date",
                "date"
            )
        ]
    ),
    new Table(
        vatsID.schema,
        vatsID.name,
        [
            new Column(
                vatsID,
                "vat_value",
                "numeric"
            )
        ]
    ),
    new Table(
        unitTypesID.schema,
        unitTypesID.name,
        [
            new Column(
                unitTypesID,
                "id",
                "integer"
            ),
            new Column(
                unitTypesID,
                "id_category",
                "integer"
            ),
            new Column(
                unitTypesID,
                "name",
                "text"
            )
        ]
    ),
    new Table(
        invoiceID.schema,
        invoiceID.name,
        [
            new Column(
                invoiceID,
                "id",
                "integer"
            ),
            new Column(
                invoiceID,
                "renomination_invoices",
                "int8[]"
            ),
            new Column(
                invoiceID,
                "payments_ids",
                "int8[]"
            ),
            new Column(
                invoiceID,
                "orders_ids",
                "bigint[]"
            )
        ]
    ),
    new Table(
        trainID.schema,
        trainID.name,
        [
            new Column(
                trainID,
                "id",
                "integer"
            ),
            new Column(
                trainID,
                "units_ids",
                "int8[]"
            )
        ]
    ),
    new Table(
        gtdID.schema,
        gtdID.name,
        [
            new Column(
                gtdID,
                "id",
                "integer"
            ),
            new Column(
                gtdID,
                "orders_ids",
                "bigint[]"
            )
        ]
    ),
    new Table(
        operationID.schema,
        operationID.name,
        [
            new Column(
                operationID,
                "id",
                "integer"
            ),
            new Column(
                operationID,
                "doc_parent_id_order",
                "bigint"
            ),
            new Column(
                operationID,
                "id_order",
                "bigint"
            ),
            new Column(
                operationID,
                "units_ids",
                "bigint[]"
            )
        ]
    ),
    new Table(
        userTaskID.schema,
        userTaskID.name,
        [
            new Column(
                userTaskID,
                "id",
                "integer"
            ),
            new Column(
                userTaskID,
                "query_name",
                "text"
            ),
            new Column(
                userTaskID,
                "row_id",
                "int8"
            )
        ]
    ),

    new Table(
        operationsID.schema,
        operationsID.name,
        [
            new Column(
                operationsID,
                "id",
                "integer"
            ),
            new Column(
                operationsID,
                "units_ids",
                "bigint[]"
            )
        ]
    )
]);