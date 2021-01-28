import { Call } from "./Call";
import { ILogStartRow, LogRow } from "./interface";

class Coach {

    private rows: LogRow[];

    constructor(rows: LogRow[]) {
        this.rows = rows;
    }

    parseCalls() {
        const rootCalls: Call[] = [];
        const parents: Call[] = [];
        
        for (let i = 0, n = this.rows.length; i < n; i++) {
            let row = this.rows[ i ];

            if ( isStart(row) ) {
                const call = new Call( row );

                let parent = parents[ parents.length - 1 ];
                if ( parent ) {
                    parent.addChild(call);
                }

                if ( parents.length === 0 ) {
                    rootCalls.push(call);
                }
                parents.push(call);
            }
            else {
                const lastCall = parents.pop();
                if ( !lastCall ) {
                    throw new Error("unexpected end " + JSON.stringify(row));
                }

                if ( row.end_id !== lastCall.id ) {
                    throw new Error("wrong end id " + JSON.stringify({
                        lastCall: {
                            id: lastCall.id,
                            func: lastCall.func
                        },
                        end: row
                    }));
                }

                lastCall.setEnd(row);
            }
        }

        return rootCalls;
    }

}


function isStart(row: LogRow): row is ILogStartRow {
    return row.func_name !== null;
}

export function parseCalls(logs: LogRow[]) {
    console.log("parsing logs");
    
    const coach = new Coach(logs);
    const rootCalls = coach.parseCalls();
    
    const slowCalls = rootCalls.sort((callA, callB) =>
        callB.total_time - callA.total_time
    ).slice(0, 10);

    console.log(
        "top 10 slow calls: \n" +
        slowCalls.map(call => 
            call.func + " " + call.total_time + "ms"
        ).join("\n")
    );

    return rootCalls;
}

module.exports = {
    Coach,
    parseCalls
};