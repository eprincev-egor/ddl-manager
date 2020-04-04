import {AbstractTableModel} from "../../objects/AbstractTableModel";
import {UniqueConstraintModel} from "../../objects/UniqueConstraintModel";
import {CheckConstraintModel} from "../../objects/CheckConstraintModel";
import {ForeignKeyConstraintModel} from "../../objects/ForeignKeyConstraintModel";
import {
    CreateTable,
    Extension,
    PrimaryKeyConstraint,
    UniqueConstraint,
    CheckConstraint,
    ForeignKeyConstraint
} from "grapeql-lang";

export function prepareAbstractTable(
    filePath: string,
    tableIdentify: string,
    tableName: string,
    parsedTable: CreateTable | Extension
): AbstractTableModel<any>["TInputData"] {
        
    // table test (...) deprecated (...)
    const deprecatedColumns = parsedTable.row.deprecatedColumns.map(columnName =>
        columnName.toString()
    );

    const primaryKey = preparePrimaryKey(parsedTable);

    const uniqueConstraints = prepareUniqueConstraints(
        filePath,
        tableName,
        parsedTable
    );

    const checkConstraints = prepareCheckConstraints(
        filePath,
        tableName,
        parsedTable
    );

    const foreignKeys = prepareForeignKeys(
        filePath,
        tableName,
        parsedTable
    );

    const values = prepareValues(
        parsedTable
    );

    return {
        columns: parsedTable.row.columns.map(parseColumn => {
            const key = parseColumn.get("name").toString();
            let nulls = parseColumn.get("nulls");
            const columnDefaultExpression = parseColumn.get("default");
            let columnDefault: string | null;

            if ( columnDefaultExpression ) {
                columnDefault = columnDefaultExpression.toString();
            }

            let type = parseColumn.get("type").toString();
            const serial2realType = {
                smallserial: "smallint",
                serial: "integer",
                bigserial: "bigint"
            };
            const isSerialType = type in serial2realType;
            if ( isSerialType ) {
                type = serial2realType[ type ];
                nulls = false;
                
                if ( !columnDefault ) {
                    const tablePrefix = tableIdentify.replace(/^public\./i, "");
                    columnDefault = `nextval('${tablePrefix}_${key}_seq'::regclass)`;
                }
            }

            return {
                filePath,
                identify: key,
                key,
                type,
                default: columnDefault,
                parsed: parseColumn,
                nulls
            };
        }),
        primaryKey,
        deprecated: parsedTable.row.deprecated,
        deprecatedColumns,
        values,
        uniqueConstraints,
        checkConstraints,
        foreignKeysConstraints: foreignKeys
    };
}

function preparePrimaryKey(
    parsedTable
): string[] {

    // get primary key constraint from parsedTable.constraints
    // or from columns
    let primaryKeyConstraint = parsedTable.row.constraints.find(constraint => 
        constraint instanceof PrimaryKeyConstraint
    ) as PrimaryKeyConstraint;

    if ( !primaryKeyConstraint ) {
        parsedTable.row.columns.forEach(column => {
            if ( column.get("primaryKey") ) {
                primaryKeyConstraint = column.get("primaryKey");
            }
        });
    }
    
    let primaryKey: string[] = null;
    if ( primaryKeyConstraint ) {
        primaryKey = primaryKeyConstraint.get("primaryKey").map(column => 
            column.toString()
        );
    }

    return primaryKey;
}

function prepareUniqueConstraints(
    filePath: string,
    tableName: string,
    parsedTable: CreateTable | Extension
): UniqueConstraintModel["TInputData"][] {
 
    // table (..., constraint x unique(...))
    const parsedUniqueConstraints = parsedTable.row.constraints.filter(constraint =>
        constraint instanceof UniqueConstraint
    ) as UniqueConstraint[];

    parsedTable.row.columns.forEach(column => {
        const uniqueConstraint = column.get("unique");
        
        if ( uniqueConstraint ) {
            parsedUniqueConstraints.push(uniqueConstraint);
        }
    });

    const uniqueConstraints = parsedUniqueConstraints.map(uniqueConstraint => {
        let name = (
            uniqueConstraint.get("name") &&
            uniqueConstraint.get("name").toString()
        );
        if ( !name ) {
            name = (
                tableName + 
                "_" +
                uniqueConstraint.get("column").toString() + 
                "_key"
            );
        }

        return {
            filePath,
            identify: name,
            name,
            parsed: uniqueConstraint,
            unique: uniqueConstraint.get("unique").map(column =>
                column.toString()
            )
        };
    });
   
    return uniqueConstraints;
}

function prepareCheckConstraints(
    filePath: string,
    tableName: string,
    parsedTable: CreateTable | Extension
): CheckConstraintModel["TInputData"][] {

    const parsedCheckConstraints = parsedTable.row.constraints.filter(constraint =>
        constraint instanceof CheckConstraint
    ) as CheckConstraint[];

    parsedTable.row.columns.forEach(column => {
        const checkConstraint = column.get("check");

        if ( checkConstraint ) {
            parsedCheckConstraints.push(checkConstraint);
        }
    });

    const checkConstraints = parsedCheckConstraints.map(checkConstraint => {
        let name = (
            checkConstraint.get("name") &&
            checkConstraint.get("name").toString()
        );

        if ( !name ) {
            name = (
                tableName + 
                "_" +
                checkConstraint.get("column").toString() + 
                "_check"
            );
        }

        return {
            filePath,
            identify: name,
            name,
            check: checkConstraint.get("check").toString().trim(),
            parsed: checkConstraint
        };
    });

    return checkConstraints;
}

function prepareForeignKeys(
    filePath: string,
    tableName: string,
    parsedTable: CreateTable | Extension
): ForeignKeyConstraintModel["TInputData"][] {

    const parsedForeignKeys = parsedTable.row.constraints.filter(constraint => 
        constraint instanceof ForeignKeyConstraint
    ) as ForeignKeyConstraint[];

    parsedTable.row.columns.forEach(column => {
        const foreignKey = column.get("foreignKey");

        if ( foreignKey ) {
            parsedForeignKeys.push( foreignKey );
        }
    });

    const foreignKeys = parsedForeignKeys.map(foreignKey => {
        let name = (
            foreignKey.get("name") &&
            foreignKey.get("name").toString()
        );
        
        if ( !name ) {
            name = (
                tableName + 
                "_" +
                foreignKey.get("column").toString() + 
                "_fkey"
            );
        }

        const selfColumns = foreignKey.get("columns").map(column =>
            column.toString()
        );

        let referenceColumns = [];
        if ( foreignKey.row.referenceColumns ) {
            referenceColumns = foreignKey.get("referenceColumns").map(refColumn => 
                refColumn.toString()
            );
        }
        if ( !referenceColumns.length ) {
            referenceColumns = ["id"];
        }

        return {
            filePath,
            identify: name,
            name,
            columns: selfColumns,
            referenceTableIdentify: foreignKey.row.referenceTable.toString(),
            referenceColumns,
            parsed: foreignKey
        };
    });

    return foreignKeys;
}

// table test (...) values (...)
function prepareValues(
    parsedTable: CreateTable | Extension
): string[][] {
    const valuesRows = parsedTable.row.valuesRows;
    if ( !valuesRows || !valuesRows.length ) {
        return null;
    }
    
    const values: string[][] = [];
    for (const valuesRow of valuesRows) {
        const valuesLine = [];
        values.push( valuesLine );
        
        const parsedValues = valuesRow.get("values");
        for (const paredValue of parsedValues) {
            valuesLine.push( paredValue.toString() );
        }
    }
    
    return values;
}