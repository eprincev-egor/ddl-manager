"use strict";

module.exports = [
    {
        str: "extract(CENTURY FROM TIMESTAMP '2000-12-16 12:21:13')",
        result: {
            field: "century",
            type: {type: "timestamp"},
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(Century FROM TIMESTAMP '2000-12-16 12:21:13')",
        result: {
            field: "century",
            type: {type: "timestamp"},
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(CENTURY FROM '2000-12-16 12:21:13')",
        result: {
            field: "century",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(DAY FROM TIMESTAMP '2000-12-16 12:21:13')",
        result: {
            field: "day",
            type: {type: "timestamp"},
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(DECADE FROM '2000-12-16 12:21:13')",
        result: {
            field: "decade",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(DECADE FROM '2000-12-16 12:21:13')",
        result: {
            field: "decade",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(dow FROM '2000-12-16 12:21:13')",
        result: {
            field: "dow",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(doy FROM '2000-12-16 12:21:13')",
        result: {
            field: "doy",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(epoch FROM '2000-12-16 12:21:13')",
        result: {
            field: "epoch",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(hour FROM '2000-12-16 12:21:13')",
        result: {
            field: "hour",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(microseconds FROM '2000-12-16 12:21:13')",
        result: {
            field: "microseconds",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(millennium FROM '2000-12-16 12:21:13')",
        result: {
            field: "millennium",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(milliseconds FROM '2000-12-16 12:21:13')",
        result: {
            field: "milliseconds",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(minute FROM '2000-12-16 12:21:13')",
        result: {
            field: "minute",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(month FROM '2000-12-16 12:21:13')",
        result: {
            field: "month",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(quarter FROM '2000-12-16 12:21:13')",
        result: {
            field: "quarter",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(second FROM '2000-12-16 12:21:13')",
        result: {
            field: "second",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(timezone FROM '2000-12-16 12:21:13')",
        result: {
            field: "timezone",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(timezone_hour FROM '2000-12-16 12:21:13')",
        result: {
            field: "timezone_hour",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(timezone_minute FROM '2000-12-16 12:21:13')",
        result: {
            field: "timezone_minute",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(week FROM '2000-12-16 12:21:13')",
        result: {
            field: "week",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    },
    {
        str: "extract(year FROM '2000-12-16 12:21:13')",
        result: {
            field: "year",
            source: {elements: [
                {content: "2000-12-16 12:21:13"}
            ]}
        }
    }
];
