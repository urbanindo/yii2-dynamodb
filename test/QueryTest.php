<?php

use UrbanIndo\Yii2\DynamoDb\Query;

class QueryTest extends TestCase
{

    /**
     * @group getItemsFromResponse
     */
    public function testGetItemsFromResponseOnBatchGetItemResult()
    {
        $query = new Query();
        $query->using = Query::USING_BATCH_GET_ITEM;
        $query->from = 'b3cbe4b0-0540-327c-92f1-82dc68be9af5';
        $response = [
            'ConsumedCapacity' => [
                [
                    'CapacityUnits' => 10,
                    'Table' => [
                        'CapacityUnits' => 10,
                    ],
                    'TableName' => 'b3cbe4b0-0540-327c-92f1-82dc68be9af5'
                ]
            ],
            'Responses' => [
                'b3cbe4b0-0540-327c-92f1-82dc68be9af5' => [
                    [
                        'Field2' => [
                            'S' => 'Richard',
                        ],
                        'Brendon' => [
                            'S' => '2ce0cac8-ba40-376f-b34b-2b67db56f82f',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Louisa',
                        ],
                        'Brendon' => [
                            'S' => '46c1196a-adbe-37eb-8459-35af1fae97c5',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Onie',
                        ],
                        'Brendon' => [
                            'S' => '3c9136f5-3c59-3636-b295-980a63681218',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Andreane',
                        ],
                        'Brendon' => [
                            'S' => 'bc401cc3-2e77-34ec-8b68-6ed7eb62636d',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Dandre',
                        ],
                        'Brendon' => [
                            'S' => '53def5fd-ac60-33d9-b5c2-81c02c4e792b',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Marilou',
                        ],
                        'Brendon' => [
                            'S' => '9c1c5652-1fe9-30d3-8af2-74cc730834a5',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Isabell',
                        ],
                        'Brendon' => [
                            'S' => 'dd1a115e-2552-39b8-b0ff-5a831f5f8366',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Carmen',
                        ],
                        'Brendon' => [
                            'S' => '0a05b57f-a29f-3b00-bb4a-1002ba2ac4c7',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Joaquin',
                        ],
                        'Brendon' => [
                            'S' => 'd55dfce4-6238-3f19-a740-8c8278a38c94',
                        ],
                    ],
                    [
                        'Field2' => [
                            'S' => 'Eleanora',
                        ],
                        'Brendon' => [
                            'S' => '7b74a73b-dde5-352a-a87b-c98cb4514e61',
                        ],
                    ],
                ],
            ],
            'UnprocessedKeys' => [
            ],
        ];
        $items = $query->getItemsFromResponse($response);
        $this->assertCount(10, $items);
        $peek = array_slice($items, -1)[0];
        $this->assertArraySubset([
            '_response' => [
                'ConsumedCapacity' => []
            ]
        ], $peek);
    }

    /**
     * @group getItemsFromResponse
     */
    public function testGetItemsFromResponseOnScanResult()
    {
        $query = new Query();
        $query->using = Query::USING_SCAN;
        $response = [
            'ConsumedCapacity' => [
                [
                    'CapacityUnits' => 10,
                    'Table' => [
                        'CapacityUnits' => 10,
                    ],
                    'TableName' => 'b3cbe4b0-0540-327c-92f1-82dc68be9af5'
                ]
            ],
            'Items' => [
                [
                    'Theo' => [
                        'S' => 'f9dac574-ce4c-352a-8aed-8eeb9f5a06a8',
                    ],
                    'field4' => [
                        'N' => '0',
                    ],
                    'field3' => [
                        'N' => '9',
                    ],
                    'field2' => [
                        'N' => '9',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => '0b04d16f-306b-346d-8d36-dac2adfac959',
                    ],
                    'field4' => [
                        'N' => '2',
                    ],
                    'field3' => [
                        'N' => '5',
                    ],
                    'field2' => [
                        'N' => '5',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => '2c71f148-20e4-3077-9364-00279c3dd5de',
                    ],
                    'field4' => [
                        'N' => '1',
                    ],
                    'field3' => [
                        'N' => '7',
                    ],
                    'field2' => [
                        'N' => '7',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'b575fd82-c7f6-308d-8a4c-e932ce676052',
                    ],
                    'field4' => [
                        'N' => '2',
                    ],
                    'field3' => [
                        'N' => '8',
                    ],
                    'field2' => [
                        'N' => '8',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'b3aa0dcc-052b-32a5-afde-41ae2f7c182e',
                    ],
                    'field4' => [
                        'N' => '1',
                    ],
                    'field3' => [
                        'N' => '10',
                    ],
                    'field2' => [
                        'N' => '10',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'ffce0c31-7343-3933-9d98-dcb4790220d9',
                    ],
                    'field4' => [
                        'N' => '1',
                    ],
                    'field3' => [
                        'N' => '4',
                    ],
                    'field2' => [
                        'N' => '4',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'ee866873-b3f7-3804-b0db-163fa47dbe71',
                    ],
                    'field4' => [
                        'N' => '0',
                    ],
                    'field3' => [
                        'N' => '6',
                    ],
                    'field2' => [
                        'N' => '6',
                    ],
                ],
            ],
            'Count' => 7,
            'ScannedCount' => 10,
        ];
        $items = $query->getItemsFromResponse($response);
        $this->assertCount(7, $items);
        $peek = array_slice($items, -1)[0];
        $this->assertArraySubset([
            '_response' => [
                'ConsumedCapacity' => [],
                'Count' => 7,
                'ScannedCount' => 10,
            ]
        ], $peek);
    }

    /**
     * @group getItemsFromResponse
     */
    public function testGetItemsFromResponseOnQueryResult()
    {
        $query = new Query();
        $query->using = Query::USING_QUERY;
        $response = [
            'ConsumedCapacity' => [
                [
                    'CapacityUnits' => 10,
                    'Table' => [
                        'CapacityUnits' => 10,
                    ],
                    'TableName' => 'b3cbe4b0-0540-327c-92f1-82dc68be9af5'
                ]
            ],
            'Items' => [
                [
                    'Theo' => [
                        'S' => 'f9dac574-ce4c-352a-8aed-8eeb9f5a06a8',
                    ],
                    'field4' => [
                        'N' => '0',
                    ],
                    'field3' => [
                        'N' => '9',
                    ],
                    'field2' => [
                        'N' => '9',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => '0b04d16f-306b-346d-8d36-dac2adfac959',
                    ],
                    'field4' => [
                        'N' => '2',
                    ],
                    'field3' => [
                        'N' => '5',
                    ],
                    'field2' => [
                        'N' => '5',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => '2c71f148-20e4-3077-9364-00279c3dd5de',
                    ],
                    'field4' => [
                        'N' => '1',
                    ],
                    'field3' => [
                        'N' => '7',
                    ],
                    'field2' => [
                        'N' => '7',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'b575fd82-c7f6-308d-8a4c-e932ce676052',
                    ],
                    'field4' => [
                        'N' => '2',
                    ],
                    'field3' => [
                        'N' => '8',
                    ],
                    'field2' => [
                        'N' => '8',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'b3aa0dcc-052b-32a5-afde-41ae2f7c182e',
                    ],
                    'field4' => [
                        'N' => '1',
                    ],
                    'field3' => [
                        'N' => '10',
                    ],
                    'field2' => [
                        'N' => '10',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'ffce0c31-7343-3933-9d98-dcb4790220d9',
                    ],
                    'field4' => [
                        'N' => '1',
                    ],
                    'field3' => [
                        'N' => '4',
                    ],
                    'field2' => [
                        'N' => '4',
                    ],
                ],
                [
                    'Theo' => [
                        'S' => 'ee866873-b3f7-3804-b0db-163fa47dbe71',
                    ],
                    'field4' => [
                        'N' => '0',
                    ],
                    'field3' => [
                        'N' => '6',
                    ],
                    'field2' => [
                        'N' => '6',
                    ],
                ],
            ],
            'Count' => 7,
            'ScannedCount' => 10,
        ];
        $items = $query->getItemsFromResponse($response);
        $this->assertCount(7, $items);
        $peek = array_slice($items, -1)[0];
        $this->assertArraySubset([
            '_response' => [
                'ConsumedCapacity' => [],
                'Count' => 7,
                'ScannedCount' => 10,
            ]
        ], $peek);
    }
    
    
    /**
     * @group getItemsFromResponse
     */
    public function testGetItemsFromResponseOnGetItemResult()
    {
        $query = new Query();
        $query->using = Query::USING_GET_ITEM;
        $response = [
            'ConsumedCapacity' => [
                [
                    'CapacityUnits' => 10,
                    'Table' => [
                        'CapacityUnits' => 10,
                    ],
                    'TableName' => 'b3cbe4b0-0540-327c-92f1-82dc68be9af5'
                ]
            ],
            'Item' =>  [
                'Barry' =>  [
                    'S' => 'Sydni',
                ],
                'Field2' =>  [
                    'S' => 'Hello',
                ],
            ],
        ];
        $items = $query->getItemsFromResponse($response);
        $this->assertCount(1, $items);
        $peek = array_slice($items, -1)[0];
        $this->assertArraySubset([
            '_response' => [
                'ConsumedCapacity' => [],
            ]
        ], $peek);
    }
}
