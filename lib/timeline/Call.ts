import { ILogStartRow, ILogEndRow, ISample } from "./interface";

export class Call {

    id: number;
    func: string;
    start_time: number;
    end_time: number;
    total_time: number;
    children: Call[];
    parentId?: number;

    constructor(startRow: ILogStartRow) {
        this.id = startRow.id;
        this.func = startRow.func_name;
        this.start_time = startRow.call_time * 1000; // from sec to ms
        this.end_time = this.start_time;
        this.total_time = 0;
        this.children = [];
    }

    addChild(child: Call) {
        child.parentId = this.id;
        this.children.push(child);
    }

    setEnd(endRow: ILogEndRow) {
        this.end_time = endRow.end_time * 1000; // from sec to ms
        this.total_time = this.end_time - this.start_time;
    }

    toSample(): ISample {
        let parent = 1;
        if ( this.parentId ) {
            parent = this.parentId + 1;
        }

        return {
            id: this.id + 1,
            parent,
            callFrame: {
                functionName: this.func,
                url: this.func + ".sql",
                scriptId: 1,
                lineNumber: 1,
                columnNumber: 1
            }
        };
    }
}
