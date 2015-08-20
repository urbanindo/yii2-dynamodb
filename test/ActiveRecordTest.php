<?php

namespace test;

class ActiveRecordTest extends \PHPUnit_Framework_TestCase {
    
    
    public function testInsertAndFindOne() {
        $db = \Yii::$app->dynamodb;
        /* @var $db \UrbanIndo\Yii2\DynamoDb\Connection */
        $command = $db->createCommand();
        /* @var $command \UrbanIndo\Yii2\DynamoDb\Command */
        
        $command->createTable(\test\data\Customer::tableName(), [
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
        ]);
        $objectToInsert = new data\Customer();
        $id = \Faker\Provider\Base::randomNumber(5);
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
        
        $objectFromFind = data\Customer::findOne($id);
        /* @var $objectFromFind data\Customer */
        //$this->assertNotNull($objectFromFind);
        //$this->assertEquals($id, $objectFromFind->id);
        //$this->assertEquals($objectToInsert->name, $objectFromFind->name);
    }
}
