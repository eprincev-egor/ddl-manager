"use strict";

import fs from "fs";
import path from "path";
import { Call } from "./Call";
import { ISample } from "./interface";
import {date2timestamp} from "./utils";

export function createTimelineFile(params: {
    outputPath: string,
    rootCalls: Call[],
    name: string
}) {
    const timeline = createTimeline(params);
    const timelineFileName = generateTimelineFileName(params.name);
    const timelineFilePath = path.join(
        params.outputPath,
        timelineFileName
    );
    
    let json = (
        "[" + 
            timeline.map(event => 
                JSON.stringify(event)
            ).join(",\n") + 
        "]"
    );

    console.log(`saving report to ${timelineFilePath}`);
    fs.writeFileSync(timelineFilePath, json);
}

function createTimeline(params: {rootCalls: Call[]}) {
    const {rootCalls} = params;
    console.log("creating timeline report"); 

    const nodes: ISample[] = []; 
    const samples: number[] = []; 
    const timeDeltas: number[] = []; 
    const lines: number[] = [];

    let firstCall = rootCalls[0];
    let lastCall = rootCalls[ rootCalls.length - 1 ];
    let total_time = (lastCall.end_time - firstCall.start_time) * 1000; // to micro seconds
	
    calls2timeline({
        calls: rootCalls, 
        nodes, 
        samples, 
        timeDeltas, 
        lines,
        parentStart: firstCall.start_time,
        parentId: 1
    });

    return  [
        {pid:12380,tid:7628,ts:0,ph:"I",cat:"disabled-by-default-devtools.timeline",name:"TracingStartedInBrowser",s:"t",args:{data:{frameTreeNodeId:303,persistentIds:true,frames:[{frame:"3AD2A5EF78392FD21CDC5CCB2F0A2316",url:"index.html",name:"",processId:10940}]}}},

        {pid:10940,tid:6772,ts:0,ph:"B",cat:"devtools.timeline",name:"FunctionCall",args:{data:{frame:"3AD2A5EF78392FD21CDC5CCB2F0A2316",
            functionName:"orange_root",
            scriptId:"1",url:"",lineNumber:1,columnNumber:1}}},

        {pid:10940,tid:6772,ts:0,ph:"P",cat:"disabled-by-default-v8.cpu_profiler",name:"Profile",id:"0x1",args:{data:{startTime:0}}},

        {pid:10940,tid:6772,ts: total_time, ph:"E",cat:"devtools.timeline",name:"FunctionCall",args:{}},

        {pid:10940,tid:6028,ts:0,ph:"P",cat:"disabled-by-default-v8.cpu_profiler",name:"ProfileChunk",id:"0x1",args:{data:{cpuProfile:{nodes:[
            {callFrame:{functionName:  "(root)",  url:"",scriptId:1,lineNumber:1,columnNumber:1},  id:1   },
            
            ...nodes
            
        ],
        
        samples

        },
        
        timeDeltas,

        lines

        }}},

        {pid:10940,tid:8160,ts:0,ph:"M",cat:"__metadata",name:"num_cpus",args:{number:4}},
        {pid:10940,tid:8160,ts:0,ph:"M",cat:"__metadata",name:"process_sort_index",args:{sort_index:-5}},
        {pid:10940,tid:8160,ts:0,ph:"M",cat:"__metadata",name:"process_name",args:{name:"Renderer"}},
        {pid:10940,tid:8160,ts:0,ph:"M",cat:"__metadata",name:"process_uptime_seconds",args:{uptime:214}},
        {pid:10940,tid:6772,ts:0,ph:"M",cat:"__metadata",name:"thread_sort_index",args:{sort_index:-1}},
        {pid:10940,tid:2208,ts:0,ph:"M",cat:"__metadata",name:"thread_name",args:{name:"Compositor"}},
        {pid:10940,tid:8208,ts:0,ph:"M",cat:"__metadata",name:"thread_name",args:{name:"Chrome_ChildIOThread"}},
        {pid:10940,tid:6772,ts:0,ph:"M",cat:"__metadata",name:"thread_name",args:{name:"CrRendererMain"}}
    ];
}

function calls2timeline(params: {
    calls: Call[],
    nodes: ISample[],
    samples: number[],
    timeDeltas: number[],
    lines: number[],
	
    parentStart: number,
    parentId: number
}) {
    const {
        calls,
        nodes,
        samples,
        timeDeltas,
        lines,
        
        parentStart,
        parentId
    } = params;


    if ( !calls.length ) {
        return;
    }
    let prevEnd = parentStart;
	
    for (let i = 0, n = calls.length; i < n; i++) {
        const call = calls[i];
        const sample = call.toSample();
        nodes.push( sample );
		
		
        let delta = call.start_time - prevEnd;
        samples.push( sample.id );
        lines.push( 1 );
        timeDeltas.push( delta * 1000 ); // ms to microSeconds
		
        calls2timeline({
            calls: call.children,
            nodes,
            samples, 
            timeDeltas, 
            lines,
            parentStart: call.start_time,
            parentId: sample.id
        });
		
		
        let lastChildEndTime = call.start_time;
        if ( call.children.length ) {
            let lastChild = call.children[ call.children.length - 1 ];
            lastChildEndTime = lastChild.end_time;
        }
        delta = call.end_time - lastChildEndTime;
		
        samples.push( parentId );
        lines.push( 1 );
        timeDeltas.push( delta * 1000 ); // ms to microSeconds
		
        prevEnd = call.end_time;
    }

}


function generateTimelineFileName(name: string) {
    const now = new Date();
    const timestamp = date2timestamp(now);
    return `timeline-${name}-${ timestamp }.json`;
}

