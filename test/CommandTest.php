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
        
        
        
    }
    
    public function testGetItemUsingCompositeKey()
    {
        
    }
}

