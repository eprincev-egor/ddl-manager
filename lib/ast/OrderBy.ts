import { flatMap } from "lodash";
import { TableID } from "../database/schema/TableID";
import { TableReference } from "../database/schema/TableReference";
import { AbstractAstElement } from "./AbstractAstElement";
import { ConditionElementType } from "./expression";
import { OrderByItem, CompareRow } from "./OrderByItem";
import { Spaces } from "./Spaces";

export class OrderBy extends AbstractAstElement {

    readonly items: readonly OrderByItem[];
    constructor(items: OrderByItem[]) {
        super();
        this.items = items;
    }

    clone() {
        const newItems = this.items.map(item => item.clone());
        return new OrderBy(newItems);
    }

    replaceTable(
        replaceTable: TableReference | TableID,
        toTable: TableReference
    ) {
        const newItems = this.items.map(item => 
            item.replaceTable(replaceTable, toTable)
        );
        return new OrderBy(newItems);
    }

    equal(orderBy: OrderBy) {
        return (
            this.items.length === orderBy.items.length &&
            this.items.every((item, i) =>
                item.equal(orderBy.items[i])
            )
        );
    }

    getColumnReferences() {
        return flatMap(this.items, item => 
            item.getColumnReferences()
        );
    }

    template(spaces: Spaces): string[] {
        return [
            spaces + "order by",
            ...this.items.map((item, i) =>
                item.toSQL( spaces.plusOneLevel() ) + (
                    i === this.items.length - 1 ? "" : ","
                )
            )
        ];
    }

    isOnlyId() {
        const orderByColumns = this.getColumnReferences();
        const byId = (
            orderByColumns.length === 1 &&
            orderByColumns[0].name === "id"
        );
        return byId;
    }

    getFirstColumnRef() {
        return this.getColumnReferences()[0];
    }

    compareRowsByOrder(
        leftRow: CompareRow,
        vector: "above" | "below",
        rightRow: CompareRow,
        orPreConditions: ConditionElementType[] = []
    ) {
        return this.items[0]!.compareRowsByOrder(
            leftRow,
            vector,
            rightRow,
            orPreConditions
        );
    }
}