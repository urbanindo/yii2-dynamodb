<?php

class CommandTest extends TestCase
{
    
    /**
     * @group createTable
     */
    public function testCreateTable()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $this->assertFalse($command->tableExists($tableName));
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $this->assertTrue($command->tableExists($tableName));
        
        $result = $command->describeTable($tableName)->execute();
        $this->assertArraySubset([
            'Table' => [
                'AttributeDefinitions' => [
                    [
                        'AttributeName' => $fieldName1,
                        'AttributeType' => 'S',
                    ]
                ],
                'KeySchema' => [
                    [
                        'AttributeName' => $fieldName1,
                        'KeyType' => 'HASH',
                    ]
                ],
                'TableName' => $tableName,
            ]
        ], $result);
    }
    
    /**
     * @group deleteTable
     */
    public function testDeleteTable()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $this->assertFalse($command->tableExists($tableName));
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $this->assertTrue($command->tableExists($tableName));
        
        $command->deleteTable($tableName)->execute();
        
        $this->assertFalse($command->tableExists($tableName));
    }
    
    /**
     * @group putItem
     */
    public function testPutItem()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $this->assertNotFalse($command->tableExists($tableName));
        
        $this->assertEquals(0, $this->getTableItemCount($tableName));
        
        $command->putItem($tableName, [
            $fieldName1 => $faker->firstNameFemale,
        ])->execute();
        
        $this->assertEquals(1, $this->getTableItemCount($tableName));
        
        $command->putItem($tableName, [
            $fieldName1 => $faker->firstNameFemale,
            'Field2' => 'Hello',
        ])->execute();

        $this->assertEquals(2, $this->getTableItemCount($tableName));
    }
    
    private function getTableItemCount($tableName) {
        $tableDescription = $this->getConnection()->createCommand()->describeTable($tableName)->execute();
        return $tableDescription['Table']['ItemCount'];
    }
    
    /**
     * @group getItem
     */
    public function testGetItemUsingScalarKey()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $id = $faker->firstNameFemale;
        $value = [
            $fieldName1 => $id,
            'Field2' => 'Hello',
        ];
        $command->putItem($tableName, $value)->execute();
        
        $result = $command->getItem($tableName, $id)->execute();

        $this->assertNotEmpty($result);
        
    }
    
    /**
     * @group getItem
     */
    public function testGetItemUsingCompositeIndexedArrayKeyWithOneElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $id = $faker->firstNameFemale;
        $value = [
            $fieldName1 => $id,
            'Field2' => 'Hello',
        ];
        $command->putItem($tableName, $value)->execute();
        
        $result = $command->getItem($tableName, [$id])->execute();
        
        $this->assertNotEmpty($result);
    }
    
    /**
     * @group getItem
     */
    public function testGetItemUsingCompositeIndexedArrayKeyWithTwoElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameFemale;
        $fieldName2 = $faker->firstNameFemale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'KeyType' => 'RANGE',
                ],
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $value = [
            $fieldName1 => $id1,
            $fieldName2 => $id2,
            'Field2' => 'Hello',
        ];
        $command->putItem($tableName, $value)->execute();
        
        $result = $command->getItem($tableName, [$id1, $id2])->execute();
        
        $this->assertNotEmpty($result);
    }
    
    /**
     * @group getItem
     */
    public function testGetItemUsingCompositeAssociativeArrayKeyWithOneElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $id = $faker->firstNameFemale;
        $value = [
            $fieldName1 => $id,
            'Field2' => 'Hello',
        ];
        $command->putItem($tableName, $value)->execute();
        
        $result = $command->getItem($tableName, [$fieldName1 => $id])->execute();
        
        $this->assertNotEmpty($result);
    }
    
    /**
     * @group getItem
     */
    public function testGetItemUsingCompositeAssociativeArrayKeyWithTwoElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameFemale;
        $fieldName2 = $faker->firstNameFemale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'KeyType' => 'RANGE',
                ],
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $value = [
            $fieldName1 => $id1,
            $fieldName2 => $id2,
            'Field2' => 'Hello',
        ];
        $command->putItem($tableName, $value)->execute();
        
        $result = $command->getItem($tableName, [$fieldName1 => $id1, $fieldName2 => $id2])->execute();
        
        $this->assertNotEmpty($result);
    }
    
    /**
     * @group batchGetItem
     */
    public function testBatchGetItemUsingIndexedArrayOfScalarElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $ids = array_map(function() use ($faker) {
            return $faker->uuid;
        }, range(1, 10));
        
        
        $values = array_map(function($id) use ($fieldName1, $faker) {
            return [
                $fieldName1 => $id,
                'Field2' => $faker->firstName,
            ];
        }, $ids);
        foreach ($values as $value) {
            $command->putItem($tableName, $value)->execute();
        }
        $this->assertEquals(10, $this->getTableItemCount($tableName));
        
        $result = $command->batchGetItem($tableName, $ids)->execute();

        $this->assertNotEmpty($result);
        $this->assertEmpty($result['UnprocessedKeys']);
    }
    
    /**
     * @group batchGetItem
     */
    public function testBatchGetItemUsingIndexedArrayOfIndexedArrayOnOneKey()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $ids = array_map(function() use ($faker) {
            return [
                $faker->uuid,
            ];
        }, range(1, 10));
        
        $values = array_map(function($id) use ($fieldName1, $faker) {
            return [
                $fieldName1 => $id[0],
                'Field2' => $faker->firstName,
            ];
        }, $ids);
        foreach ($values as $value) {
            $command->putItem($tableName, $value)->execute();
        }
        $this->assertEquals(10, $this->getTableItemCount($tableName));
        
        $result = $command->batchGetItem($tableName, $ids)->execute();
        
        $this->assertNotEmpty($result);
        $this->assertEmpty($result['UnprocessedKeys']);
    }
    
    /**
     * @group batchGetItem
     */
    public function testBatchGetItemUsingIndexedArrayOfIndexedArrayOnTwoKey()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        $fieldName2 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'KeyType' => 'RANGE',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $ids = array_map(function() use ($faker) {
            return [
                $faker->uuid,
                $faker->uuid,
            ];
        }, range(1, 10));
        
        $values = array_map(function($id) use ($fieldName1, $fieldName2, $faker) {
            return [
                $fieldName1 => $id[0],
                $fieldName2 => $id[1],
                'Field2' => $faker->firstName,
            ];
        }, $ids);
        foreach ($values as $value) {
            $command->putItem($tableName, $value)->execute();
        }
        $this->assertEquals(10, $this->getTableItemCount($tableName));
        
        $result = $command->batchGetItem($tableName, $ids)->execute();
        
        $this->assertNotEmpty($result);
        $this->assertEmpty($result['UnprocessedKeys']);
    }
    
    /**
     * @group batchGetItem
     */
    public function testBatchGetItemUsingIndexedArrayOfAssociativeArray()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        $fieldName2 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'KeyType' => 'RANGE',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $ids = array_map(function() use ($fieldName1, $fieldName2, $faker) {
            return [
                $fieldName1 => $faker->uuid,
                $fieldName2 => $faker->uuid,
            ];
        }, range(1, 10));
        
        $values = array_map(function($id) use ($faker) {
            return array_merge($id, [
                'Field2' => $faker->firstName,
            ]);
        }, $ids);
        foreach ($values as $value) {
            $command->putItem($tableName, $value)->execute();
        }
        $this->assertEquals(10, $this->getTableItemCount($tableName));
        
        $result = $command->batchGetItem($tableName, $ids)->execute();
        $this->assertNotEmpty($result);
        $this->assertEmpty($result['UnprocessedKeys']);
    }
    
    /**
     * @group batchGetItem
     */
    public function testBatchGetItemUsingAssociativeArrayOnTwoKeys()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        $fieldName2 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'KeyType' => 'RANGE',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ],
                [
                    'AttributeName' => $fieldName2,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        $ids = [];
        $values = [];
        foreach (range(1, 10) as $i) {
            $field1 = $faker->uuid;
            $field2 = $faker->uuid;
            $ids[$fieldName1][] = $field1;
            $ids[$fieldName2][] = $field2;
            $values[] = [
                $fieldName1 => $field1,
                $fieldName2 => $field2,
            ];
        }
        
        foreach ($values as $value) {
            $command->putItem($tableName, $value)->execute();
        }
        
        $this->assertEquals(10, $this->getTableItemCount($tableName));
        
        $result = $command->batchGetItem($tableName, $ids)->execute();
        $this->assertNotEmpty($result);
        $this->assertEmpty($result['UnprocessedKeys']);
    }
    
    /**
     * @group scan
     */
    public function testScan()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->uuid;
        $fieldName1 = $faker->firstNameMale;
        
        $command->createTable($tableName, [
            'KeySchema' => [
                [
                    'AttributeName' => $fieldName1,
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => $fieldName1,
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ])->execute();
        
        foreach (range(1, 10) as $i) {
            $command->putItem($tableName, [
                $fieldName1 => $faker->uuid,
                'field2' => $i,
                'field3' => $i,
                'field4' => $i % 3,
            ])->execute();
        }
        
        $result1 = $command->scan($tableName, [
            'FilterExpression' => 'field2 > :val1',
            'ExpressionAttributeValues' => [
                ':val1' => ['N' => '3']
            ],
        ])->execute();
        
        $this->assertNotEmpty($result1);
        $this->assertEquals(7, $result1['Count']);
        
        $result2 = $command->scan($tableName, [
            'FilterExpression' => 'field2 > :val1 AND field2 < :val2',
            'ExpressionAttributeValues' => [
                ':val1' => ['N' => '3'],
                ':val2' => ['N' => '6']
            ],
        ])->execute();
        
        $this->assertNotEmpty($result2);
        $this->assertEquals(2, $result2['Count']);
        
        $result3 = $command->scan($tableName, [
            'FilterExpression' => 'field2 > :val1 AND field4 = :val2',
            'ExpressionAttributeValues' => [
                ':val1' => ['N' => '3'],
                ':val2' => ['N' => '2']
            ],
        ])->execute();
        
        $this->assertNotEmpty($result3);
        $this->assertEquals(2, $result3['Count']);
        
        $result4 = $command->scan($tableName, [
            'FilterExpression' => 'field4 = :val1 OR field1 = :val2',
            'ExpressionAttributeValues' => [
                ':val1' => ['N' => '2'],
                ':val2' => ['N' => '4']
            ],
        ])->execute();
        
        $this->assertNotEmpty($result4);
        $this->assertEquals(3, $result4['Count']);
        
        $result5 = $command->scan($tableName, [
            'FilterExpression' => 'field4 = :val1 AND (field1 = :val2 OR field2 = :val3)',
            'ExpressionAttributeValues' => [
                ':val1' => ['N' => '2'],
                ':val2' => ['N' => '4'],
                ':val3' => ['N' => '5']
            ],
        ])->execute();
        
        $this->assertNotEmpty($result5);
        $this->assertEquals(1, $result5['Count']);
    }
}

