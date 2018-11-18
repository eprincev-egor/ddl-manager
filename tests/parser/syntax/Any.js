"use strict";

module.exports = [
    {
        str: "any(array[1])",
        result: {
            type: "any",
            array: {elements: [
                {items: [
                    {elements: [
                        {number: "1"}
                    ]}
                ]}
            ]}
        }
    },
    {
        str: "some(array[1])",
        result: {
            type: "some",
            array: {elements: [
                {items: [
                    {elements: [
                        {number: "1"}
                    ]}
                ]}
            ]}
        }
    },
    {
        str: "all(array[1])",
        result: {
            type: "all",
            array: {elements: [
                {items: [
                    {elements: [
                        {number: "1"}
                    ]}
                ]}
            ]}
        }
    }
];
