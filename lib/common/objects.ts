
export interface IDBO {
    getIdentify(): string;
    toDropSQL(): string;
    toCreateSQL(): string;
}

export interface IColumnDBO extends IDBO {
    getDefaultSQL(): string;
    getTypeSQL(): string;
    getNulls(): boolean;
}

export interface ITableDBO extends IDBO {
    getColumns(): IColumnDBO[];
}

export interface IExtension {
    getColumns(): IColumnDBO[];
}

export type TDBO = (
    IDBO |
    IExtension
);