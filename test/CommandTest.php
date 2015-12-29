<?php

class CommandTest extends TestCase
{
    
    public function testCreateTable()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
    
    public function testDeleteTable()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
    
    public function testPutItem()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
    
    public function testGetItemUsingScalarKey()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
        
        $result = $command->getItem($tableName, $id)->queryOne();
        
        $this->assertEquals($value, $result);
        
    }
    
    public function testGetItemUsingCompositeIndexedArrayKeyWithOneElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
        
        $result = $command->getItem($tableName, [$id])->queryOne();
        
        $this->assertEquals($value, $result);
    }
    
    public function testGetItemUsingCompositeIndexedArrayKeyWithTwoElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
        
        $result = $command->getItem($tableName, [$id1, $id2])->queryOne();
        
        $this->assertEquals($value, $result);
    }
    
    public function testGetItemUsingCompositeAssociativeArrayKeyWithOneElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
        
        $result = $command->getItem($tableName, [$fieldName1 => $id])->queryOne();
        
        $this->assertEquals($value, $result);
    }
    
    public function testGetItemUsingCompositeAssociativeArrayKeyWithTwoElement()
    {
        $db = $this->getConnection();
        $command = $db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = $faker->firstNameMale;
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
        
        $result = $command->getItem($tableName, [$fieldName1 => $id1, $fieldName2 => $id2])->queryOne();
        
        $this->assertEquals($value, $result);
    }
}

