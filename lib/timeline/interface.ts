
export type LogRow = ILogStartRow | ILogEndRow;

export interface ILogStartRow {
    id: number;
    func_name: string;
    call_time: number;
    end_id: null;
}

export interface ILogEndRow {
    call_time: null;
    func_name: null;
    end_id: number;
    end_time: number;
}

export interface ISample {
    id: number;
    parent?: number;
    callFrame: {
        codeType: string;
        functionName: string;
        url?: string;
        scriptId: number;
        lineNumber?: number;
        columnNumber?: number;
    }
}