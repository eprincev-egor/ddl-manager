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
    
    let json = JSON.stringify(timeline);

    console.log(`saving report to ${timelineFilePath}`);
    fs.writeFileSync(timelineFilePath, json);
}

function createTimeline(params: {
    rootCalls: Call[],
    name: string;
}) {
    const {rootCalls} = params;
    console.log("creating timeline report"); 

    const nodes: ISample[] = [{
        callFrame: {
            codeType: "other",
            functionName: "(root)",
            scriptId: 0
        },
        "id": 1
    }]; 
    const samples: number[] = []; 
    const timeDeltas: number[] = []; 

    let firstCall = rootCalls[0];
    let lastCall = rootCalls[ rootCalls.length - 1 ];
    let total_time = (lastCall.end_time - firstCall.start_time) * 1000; // to micro seconds
	
    calls2timeline({
        calls: rootCalls, 
        nodes, 
        samples, 
        timeDeltas,
        parentStart: firstCall.start_time,
        parentId: 1
    });

    return  {"traceEvents": [
        {"args":{"name":"swapper"},"cat":"__metadata","name":"thread_name","ph":"M","pid":0,"tid":0,"ts":0},
        {"args":{"name":"CrRendererMain"},"cat":"__metadata","name":"thread_name","ph":"M","pid":31577,"tid":1,"ts":0},
        {"args":{"name":"ThreadPoolServiceThread"},"cat":"__metadata","name":"thread_name","ph":"M","pid":31577,"tid":2,"ts":0},
        {"args":{"name":"CrBrowserMain"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5310,"tid":5310,"ts":0},
        {"args":{"name":"Chrome_IOThread"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5310,"tid":5328,"ts":0},
        {"args":{"name":"CrGpuMain"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5344,"tid":5344,"ts":0},
        {"args":{"name":"ThreadPoolServiceThread"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5344,"tid":5424,"ts":0},
        {"args":{"name":"VizCompositorThread"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5344,"tid":5428,"ts":0},
        {"args":{"name":"Chrome_ChildIOThread"},"cat":"__metadata","name":"thread_name","ph":"M","pid":31577,"tid":4,"ts":0},
        {"args":{"name":"v8:ProfEvntProc"},"cat":"__metadata","name":"thread_name","ph":"M","pid":31577,"tid":22,"ts":0},
        {"args":{"name":"ThreadPoolForegroundWorker"},"cat":"__metadata","name":"thread_name","ph":"M","pid":31577,"tid":3,"ts":0},
        {"args":{"name":"ThreadPoolForegroundWorker"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5310,"tid":5325,"ts":0},
        {"args":{"name":"ThreadPoolForegroundWorker"},"cat":"__metadata","name":"thread_name","ph":"M","pid":5344,"tid":5425,"ts":0},
        {"args":{"name":"Renderer"},"cat":"__metadata","name":"process_name","ph":"M","pid":31577,"tid":0,"ts":0},
        {"args":{"name":"Browser"},"cat":"__metadata","name":"process_name","ph":"M","pid":5310,"tid":0,"ts":0},
        {"args":{"name":"GPU Process"},"cat":"__metadata","name":"process_name","ph":"M","pid":5344,"tid":0,"ts":0},
        {"args":{"uptime":"198"},"cat":"__metadata","name":"process_uptime_seconds","ph":"M","pid":31577,"tid":0,"ts":0},
        {"args":{"uptime":"12287"},"cat":"__metadata","name":"process_uptime_seconds","ph":"M","pid":5310,"tid":0,"ts":0},
        {"args":{"uptime":"12287"},"cat":"__metadata","name":"process_uptime_seconds","ph":"M","pid":5344,"tid":0,"ts":0},
        {"args":{"data":{"frameTreeNodeId":224,"frames":[{"frame":"3D5D20F86E81F62ADC8CFC7C151E0D88","name":"","processId":31577,"url":"root"}],"persistentIds":true}},
          "cat":"disabled-by-default-devtools.timeline","name":"TracingStartedInBrowser","ph":"I","pid":5310,"s":"t","tid":5310,"ts":0,"tts":0},
        
        {"args":{"data":{"type": params.name + ".sql"}},"cat":"devtools.timeline",
          "dur":total_time,
          "name":"EventDispatch","ph":"X","pid":31577,"tdur":0,"tid":1,"ts":0,"tts":0,"selfTime":0},
        {
              "args": {
                  "data": {
                      "frame": "3D5D20F86E81F62ADC8CFC7C151E0D88",
                      "columnNumber": 17,
                      "functionName": "root",
                      "lineNumber": 1,
                      "scriptId": "-101",
                      "url": "root"
                  }
              },
              "cat": "devtools.timeline",
              "dur": 8394,
              "name": "FunctionCall",
              "ph": "X",
              "pid": 31577,
              "tdur": total_time,
              "tid": 1,
              "ts": 1,
              "tts": 0,
              "selfTime": 0
          },
          {
              "args": {
                  "data": {
                      "startTime": 3
                  }
              },
              "cat": "disabled-by-default-v8.cpu_profiler",
              "id": "0x5",
              "name": "Profile",
              "ph": "P",
              "pid": 31577,
              "tid": 1,
              "ts": 1,
              "tts": 0
          },
          {
              "args": {
                  "data": {
                      "cpuProfile": {
                          "nodes": nodes,
                          "samples": samples
                      },
                      "timeDeltas": timeDeltas
                  }
              },
              "cat": "disabled-by-default-v8.cpu_profiler",
              "id": "0x5",
              "name": "ProfileChunk",
              "ph": "P",
              "pid": 31577,
              "tid": 22,
              "ts": 2,
              "tts": 0
          },
          {"args":{"data":{"type": "end timeline"}},"cat":"devtools.timeline",
          "dur": 1,
          "name":"EventDispatch","ph":"X","pid":31577,"tdur":0,"tid":1,"ts":total_time,"tts":0,"selfTime":0}
      ],
      "metadata": {
        "source": "DevTools",
        "startTime": "2024-03-18T15:43:05.251Z",
        "cpuThrottling": 1,
        "networkThrottling": "No throttling",
        "hardwareConcurrency": 20,
        "dataOrigin": "TraceEvents"
      }};
}

function calls2timeline(params: {
    calls: Call[],
    nodes: ISample[],
    samples: number[],
    timeDeltas: number[],
	
    parentStart: number,
    parentId: number
}) {
    const {
        calls,
        nodes,
        samples,
        timeDeltas,
        
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
        timeDeltas.push( delta * 1000 ); // ms to microSeconds

        calls2timeline({
            calls: call.children,
            nodes,
            samples, 
            timeDeltas,
            parentStart: call.start_time,
            parentId: sample.id
        });
        console.log(call.children);

        delta = call.end_time - call.getLastChildEndTime();

        samples.push( parentId );
        timeDeltas.push( delta * 1000 ); // ms to microSeconds
		
        prevEnd = call.end_time;
    }

}


function generateTimelineFileName(name: string) {
    const now = new Date();
    const timestamp = date2timestamp(now);
    return `timeline-${name}-${ timestamp }.json`;
}

