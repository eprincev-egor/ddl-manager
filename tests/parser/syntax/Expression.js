"use strict";

module.exports = [
    {
        str: "1 + 1",
        result: {
            elements: [
                {number: "1"},
                {operator: "+"},
                {number: "1"}
            ]
        }
    },
    {
        str: "1::text::bigint-2 ",
        result: {
            elements: [
                {number: "1"},
                {dataType: {type: "text"}},
                {dataType: {type: "bigint"}},
                {operator: "-"},
                {number: "2"}
            ]
        }
    },
    {
        str: "user::bigint % 4",
        result: {
            elements: [
                {link: [
                    {word: "user"}
                ]},
                {dataType: {type: "bigint"}},
                {operator: "%"},
                {number: "4"}
            ]
        }
    },
    {
        str: "$$test$$ || E'str'",
        result: {
            elements: [
                {content: "test"},
                {operator: "||"},
                {content: "str"}
            ]
        }
    },
    {
        str: "true-false*null+1/'test'",
        result: {
            elements: [
                {boolean: true},
                {operator: "-"},
                {boolean: false},
                {operator: "*"},
                {null: true},
                {operator: "+"},
                {number: "1"},
                {operator: "/"},
                {content: "test"}
            ]
        }
    },
    {
        str: "((('extrude')))",
        result: {
            elements: [
                {content: "extrude"}
            ]
        }
    },
    {
        str: "(-1+2.1)*''-(('test')+8)",
        result: {
            elements: [
                {
                    elements: [
                        {operator: "-"},
                        {number: "1"},
                        {operator: "+"},
                        {number: "2.1"}
                    ]
                },
                {operator: "*"},
                {content: ""},
                {operator: "-"},
                {
                    elements: [
                        {elements: [
                            {content: "test"}
                        ]},
                        {operator: "+"},
                        {number: "8"}
                    ]
                }
            ]
        }
    },
    {
        str: "order.sum + Company.total",
        result: {
            elements: [
                {link: [
                    {word: "order"},
                    {word: "sum"}
                ]},
                {operator: "+"},
                {link: [
                    {word: "Company"},
                    {word: "total"}
                ]}
            ]
        }
    },
    {
        str: "now()::date + 3",
        result: {
            elements: [
                {
                    "function": {link: [{ word: "now" }]},
                    "arguments": []
                },
                {
                    dataType: {type: "date"}
                },
                {
                    operator: "+"
                },
                {
                    number: "3"
                }
            ]
        }
    },
    {
        str: "id = 2 or id = -3",
        result: {
            elements: [
                {link: [
                    {word: "id"}
                ]},
                {operator: "="},
                {number: "2"},

                {operator: "or"},

                {link: [
                    {word: "id"}
                ]},
                {operator: "="},
                {operator: "-"},
                {number: "3"}
            ]
        }
    },
    {
        str: "id = 2 or - -+id = 3",
        result: {
            elements: [
                {link: [
                    {word: "id"}
                ]},
                {operator: "="},
                {number: "2"},

                {operator: "or"},

                {operator: "-"},
                {operator: "-"},
                {operator: "+"},

                {link: [
                    {word: "id"}
                ]},
                {operator: "="},
                {number: "3"}
            ]
        }
    },
    {
        str: "2::date::text || '120'::char(2) - -8",
        result: {
            elements: [
                {number: "2"},
                {dataType: {type: "date"}},
                {dataType: {type: "text"}},

                {operator: "||"},

                {content: "120"},
                {dataType: {type: "char(2)"}},

                {operator: "-"},

                {operator: "-"},
                {number: "8"}
            ]
        }
    },
    {
        str: "(-1 + 2.1) * '0'::numeric - ( ('-2')::bigint + 8)",
        result: {
            elements: [
                {elements: [
                    {operator: "-"},
                    {number: "1"},
                    {operator: "+"},
                    {number: "2.1"}
                ]},

                {operator: "*"},

                {content: "0"},
                {dataType: {type: "numeric"}},

                {operator: "-"},

                {elements: [
                    {elements: [
                        {content: "-2"}
                    ]},

                    {dataType: {type: "bigint"}},
                    {operator: "+"},
                    {number: "8"}
                ]}
            ]
        }
    },
    {
        str: "company.id in (1, (1+ 1),3)",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {in: [
                    {elements: [
                        {number: "1"}
                    ]},

                    {elements: [
                        {number: "1"},
                        {operator: "+"},
                        {number: "1"}
                    ]},

                    {elements: [
                        {number: "3"}
                    ]}
                ]}
            ]
        }
    },
    {
        str: "company.id between 1 and 2",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"}
                    ]}
                }
            ]
        }
    },
    {
        str: "company.id between 1 and 2 or company.id between 5 and 6",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"}
                    ]}
                },
                {operator: "or"},
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "5"}
                    ]},
                    end: {elements: [
                        {number: "6"}
                    ]}
                }
            ]
        }
    },
    {
        str: "company.id between 1 and 2 > true",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"}
                    ]}
                },
                {operator: ">"},
                {boolean: true}
            ]
        }
    },
    {
        str: "company.id between 1 and 2 >= true",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"}
                    ]}
                },
                {operator: ">="},
                {boolean: true}
            ]
        }
    },
    {
        str: "company.id between 1 and 2 < true",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"}
                    ]}
                },
                {operator: "<"},
                {boolean: true}
            ]
        }
    },
    {
        str: "company.id between 1 and 2 + 3 <= true",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"},
                        {operator: "+"},
                        {number: "3"}
                    ]}
                },
                {operator: "<="},
                {boolean: true}
            ]
        }
    },
    {
        str: "company.id not between 1 and 2 + 3",
        result: {
            elements: [
                {link: [
                    {word: "company"},
                    {word: "id"}
                ]},
                {operator: "not"},
                {
                    start: {elements: [
                        {number: "1"}
                    ]},
                    end: {elements: [
                        {number: "2"},
                        {operator: "+"},
                        {number: "3"}
                    ]}
                }
            ]
        }
    },
    {
        str: "id not in (1,2)",
        result: {
            elements: [
                {link: [
                    {word : "id"}
                ]},
                {operator: "not"},
                {in: [
                    {elements: [
                        {number: "1"}
                    ]},
                    {elements: [
                        {number: "2"}
                    ]}
                ]}
            ]
        }
    },
    {
        str: "test.id between 1 + 3 and 3 + 1 > test.id between ( 8 * test.id ) and 30 - 8",
        result: {
            elements: [
                {link: [
                    {word: "test"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "1"},
                        {operator: "+"},
                        {number: "3"}
                    ]},
                    end: {elements: [
                        {number: "3"},
                        {operator: "+"},
                        {number: "1"}
                    ]}
                },

                {operator: ">"},

                {link: [
                    {word: "test"},
                    {word: "id"}
                ]},
                {
                    start: {elements: [
                        {number: "8"},
                        {operator: "*"},
                        {link: [
                            {word: "test"},
                            {word: "id"}
                        ]}
                    ]},
                    end: {elements: [
                        {number: "30"},
                        {operator: "-"},
                        {number: "8"}
                    ]}
                }
            ]
        }
    },
    {
        str: "is true",
        result: {
            elements: [
                {operator: "is"},
                {boolean: true}
            ]
        }
    },
    {
        str: "is not true",
        result: {
            elements: [
                {operator: "is not"},
                {boolean: true}
            ]
        }
    },
    {
        str: "is false",
        result: {
            elements: [
                {operator: "is"},
                {boolean: false}
            ]
        }
    },
    {
        str: "is not false",
        result: {
            elements: [
                {operator: "is not"},
                {boolean: false}
            ]
        }
    },
    {
        str: "'abc' ~ 'abc'",
        result: {
            elements: [
                {content: "abc"},
                {operator: "~"},
                {content: "abc"}
            ]
        }
    },
    {
        str: "array[]",
        result: {
            elements: [
                {items: []}
            ]
        }
    },
    {
        str: "array[1]",
        result: {
            elements: [
                {items: [
                    {
                        elements: [
                            {number: "1"}
                        ]
                    }
                ]}
            ]
        }
    },
    {
        str: "array[true, false]",
        result: {
            elements: [
                {items: [
                    {
                        elements: [
                            {boolean: true}
                        ]
                    },
                    {
                        elements: [
                            {boolean: false}
                        ]
                    }
                ]}
            ]
        }
    },
    {
        str: "array[1]::bigint[]",
        result: {
            elements: [
                {items: [
                    {
                        elements: [
                            {number: "1"}
                        ]
                    }
                ]},
                {dataType: {type: "bigint[]"}}
            ]
        }
    },
    {
        str: "array[null]::bigint[]",
        result: {
            elements: [
                {items: [
                    {
                        elements: [
                            {null: true}
                        ]
                    }
                ]},
                {dataType: {type: "bigint[]"}}
            ]
        }
    },
    {
        str: "array[null]::numeric(10, 2)[]",
        result: {
            elements: [
                {items: [
                    {
                        elements: [
                            {null: true}
                        ]
                    }
                ]},
                {dataType: {type: "numeric(10,2)[]"}}
            ]
        }
    },
    {
        str: "extract( century from Timestamp '2000-12-16 12:21:14' )",
        result: {
            elements: [{
                field: "century",
                type: {type: "timestamp"},
                source: {elements: [
                    {content: "2000-12-16 12:21:14"}
                ]}
            }]
        }
    },
    {
        str: "my_table.my_column_arr[1]",
        result: {
            elements: [
                {link: [
                    {word: "my_table"},
                    {word: "my_column_arr"}
                ]},
                {content: {elements: [
                    {number: "1"}
                ]}}
            ]
        }
    },
    {
        str: "(array[100, 200])[1]",
        result: {
            elements: [
                {elements: [
                    {items: [
                        {elements: [
                            {number: "100"}
                        ]},
                        {elements: [
                            {number: "200"}
                        ]}
                    ]}
                ]},
                {
                    content: {elements: [
                        {number: "1"}
                    ]}
                }
            ]
        }
    },
    {
        str: "(array[100, 200]::bigint[])[1]",
        result: {
            elements: [
                {elements: [
                    {items: [
                        {elements: [
                            {number: "100"}
                        ]},
                        {elements: [
                            {number: "200"}
                        ]}
                    ]},
                    {dataType: {type: "bigint[]"}}
                ]},
                {
                    content: {elements: [
                        {number: "1"}
                    ]}
                }
            ]
        }
    },
    {
        str: "substring('123456' from 2 for 3)",
        result: {
            elements: [
                {
                    str: {elements: [{
                        content: "123456"
                    }]},
                    from: {elements: [{
                        number: "2"
                    }]},
                    for: {elements: [{
                        number: "3"
                    }]}
                }
            ]
        }
    },
    {
        str: "any(array[1])",
        result: {
            elements: [{
                type: "any",
                array: {elements: [
                    {items: [
                        {elements: [
                            {number: "1"}
                        ]}
                    ]}
                ]}
            }]
        }
    }
];
