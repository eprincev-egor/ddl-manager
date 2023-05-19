import {
    Body,
    If,
    HardCode,
    BlankLine,
    Update,
    SetSelectItem,
    Expression
} from "../../../ast";
import { TableReference } from "../../../database/schema/TableReference";
import { exitIf } from "./util/exitIf";

export function buildSelfUpdateByOtherTablesBody(
    updateTable: TableReference,
    noChanges: Expression,
    columnsToUpdate: string[],
    selectNewValues: string,
    notMatchedFilterOnUpdate?: Expression,
) {
    const body = new Body({
        statements: [
            new BlankLine(),
            new If({
                if: noChanges,
                then: [
                    new HardCode({
                        sql: `return new;`
                    })
                ]
            }),
            ...exitIf({
                if: notMatchedFilterOnUpdate,
                blanksBefore: [new BlankLine()]
            }),
            new BlankLine(),
            new BlankLine(),
            
            new Update({
                table: updateTable.toString(),
                set: [new SetSelectItem({
                    columns: columnsToUpdate,
                    select: selectNewValues
                })],

                where: new HardCode({
                    sql: `${updateTable.getIdentifier()}.id = new.id`
                })
            }),
            
            new BlankLine(),
            new BlankLine(),
            new HardCode({
                sql: `return new;`
            })
        ]
    });

    return body;
}