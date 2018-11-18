"use strict";

module.exports = [
    {
        str: "--123\n",
        result: {content: "123"}
    },
    {
        str: "--123\r",
        result: {content: "123"}
    },
    {
        str: "/*123\n456*/",
        result: {content: "123\n456"}
    },
    {
        str: "/*123\r456*/",
        result: {content: "123\r456"}
    }
];
