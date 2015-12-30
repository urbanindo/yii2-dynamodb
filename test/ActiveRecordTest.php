<?php

class ActiveRecordTest extends TestCase {
    
    protected function setUp() {
        parent::setUp();
        $command = $this->getConnection()->createCommand();
        /* @var $command \UrbanIndo\Yii2\DynamoDb\Command */
        $table = \test\data\Customer::tableName();
        if ($command->tableExists($table)) {
            $command->deleteTable($table)->execute();
        }
        $command->createTable($table, [
            'AttributeDefinitions' => [
                [
                    'AttributeName' => 'id',
                    'AttributeType' => 'N'
                ]
            ],
            'KeySchema' => [
                [
                    'AttributeName' => 'id',
                    'KeyType' => 'HASH',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 10,
                'WriteCapacityUnits' => 10
            ]
        ])->execute();
    }
    
    public function testInsertAndFindOne() {
        
        $this->assertEquals(0, $this->getTableItemCount(\test\data\Customer::tableName()));
        $objectToInsert = new \test\data\Customer();
        $id = (int) \Faker\Provider\Base::randomNumber(5);
        $faker = \Faker\Factory::create();
        $objectToInsert->id = $id;
        $objectToInsert->name = $faker->name;
        $objectToInsert->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];
        
        $this->assertTrue($objectToInsert->save(false));
        $this->assertEquals(1, $this->getTableItemCount(\test\data\Customer::tableName()));
    }
    
    public function testInsertAndFindAll() {

        $id1 = (int) \Faker\Provider\Base::randomNumber(5);
        $faker = \Faker\Factory::create();
        $objectToInsert1 = new \test\data\Customer();
        $objectToInsert1->id = $id1;
        $objectToInsert1->name = $faker->name;
        $objectToInsert1->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert1->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert1->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];
        
        $this->assertTrue($objectToInsert1->save(false));
        
        $objectsFromFind = \test\data\Customer::findAll(['id' => [$id1]]);
       
        /* @var $objectFromFind data\Customer */
        $this->assertEquals(1, count($objectsFromFind));
        
        $id2 = (int) \Faker\Provider\Base::randomNumber(5);
        $objectToInsert2 = new \test\data\Customer();
        $objectToInsert2->id = $id2;
        $objectToInsert2->name = $faker->name;
        $objectToInsert2->contacts = [
            'telephone2' => 223456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert2->prices = [
            2000000,
            2000000,
            2000000,
            2000000
        ];
        $objectToInsert2->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];
        
        $this->assertTrue($objectToInsert2->save(false));
        
        $objectsFromFind2 = \test\data\Customer::findAll(['id' => [$id1, $id2]]);
         
         /* @var $objectFromFind data\Customer */
        $this->assertEquals(2, count($objectsFromFind2));
        
        $this->assertTrue($objectsFromFind2[0]->id = $id1 || $objectsFromFind2[0]->id = $id2);
    }
}
