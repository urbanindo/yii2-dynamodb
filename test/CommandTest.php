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
}

