"use strict";

module.exports = [
    {
        str: "in(1)",
        result: {
            in: [
                {elements: [
                    {number: "1"}
                ]}
            ]
        }
    },
    {
        str: "in ( 1 ,  'nice' )",
        result: {
            in: [
                {elements: [
                    {number: "1"}
                ]},

                {elements: [
                    {content: "nice"}
                ]}
            ]
        }
    }
];
