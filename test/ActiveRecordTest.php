<?php

namespace test;

class ActiveRecordTest extends \PHPUnit_Framework_TestCase {
    const FIELD_PROPERTY_VIEW = 1;
    const FIELD_PROPERTY_VISIT = 2;
    const FIELD_HOME_PREMIUM_VIEW = 3;
    const FIELD_HOME_PREMIUM_VISIT = 4;
    const FIELD_FEATURED_PREMIUM_VIEW = 5;
    const FIELD_FEATURED_PREMIUM_VISIT = 6;
    const FIELD_PROPERTY_MESSAGE = 7;
    const FIELD_PROPERTY_CONTACT = 8;

    public function testInsertData() {
//        $db = \Yii::$app->dynamodb;
//        $command = $db->createCommand();
//        /* @var $command \UrbanIndo\Yii2\DynamoDb\Command */
//        $command->createTable(\test\data\PropertyAnalyticsStat::tableName(), [
//            'AttributeDefinitions' => [
//                [
//                    'AttributeName' => 'propertyId',
//                    'AttributeType' => 'N'
//                ],
//                [
//                    'AttributeName' => 'day',
//                    'AttributeType' => 'S'
//                ]
//            ],
//            'KeySchema' => [
//                [
//                    'AttributeName' => 'propertyId',
//                    'KeyType' => 'HASH',
//                ],
//                [
//                    'AttributeName' => 'day',
//                    'KeyType' => 'RANGE',
//                ]
//            ],
//            'ProvisionedThroughput' => [
//                'ReadCapacityUnits' => 10,
//                'WriteCapacityUnits' => 10
//            ]
//        ]);
        $json = [];
        $filter = [
            'Property View' => self::FIELD_PROPERTY_VIEW,
            'Property Visit' => self::FIELD_PROPERTY_VISIT,
            'Home Premium View' => self::FIELD_HOME_PREMIUM_VIEW,
            'Home Premium Visit' => self::FIELD_HOME_PREMIUM_VISIT,
            'Featured Premium View' => self::FIELD_FEATURED_PREMIUM_VIEW,
            'Featured Premium Visit' => self::FIELD_FEATURED_PREMIUM_VISIT,
            'Property Message' => self::FIELD_PROPERTY_MESSAGE,
            'Property Contact' => self::FIELD_PROPERTY_CONTACT,
        ];
        
        $data = [];
        $fakestats = [];
        foreach (range(1,8) as $type) {
            $fakestats[$type] = 1000;
        }
        foreach (range(10000000, 10000025) as $i) {
            $datum = [
                'propertyId' => $i,
                'day' => '20150802',
                'value' => []
            ];
            foreach (range(0, 23) as $hour) {
                $datum['value'][$hour] = $fakestats;
            }
            $data[] = $datum;
        }
        data\PropertyAnalyticsStat::batchInsert($data);
        $x = data\PropertyAnalyticsStat::findOne([
            'propertyId' => 10000000,
            'day' => '20150802'
        ]);
        $y = data\PropertyAnalyticsStat::find()->where([
            'propertyId' => [10000000,10000001],
            'day' => ['20150802', '20150802']
        ])->using(\UrbanIndo\Yii2\DynamoDb\Query::TYPE_BATCH_GET)->all();
        
        print_r($y);
        $this->assertNotEmpty($x);
        $this->assertNotEmpty($y);
    }
    
//    public function testInsertAndFindOne() {
//        $db = \Yii::$app->dynamodb;
//        /* @var $db \UrbanIndo\Yii2\DynamoDb\Connection */
//        $command = $db->createCommand();
//        /* @var $command \UrbanIndo\Yii2\DynamoDb\Command */
//        
//        $command->createTable(\test\data\Customer::tableName(), [
//            'AttributeDefinitions' => [
//                [
//                    'AttributeName' => 'id',
//                    'AttributeType' => 'N'
//                ]
//            ],
//            'KeySchema' => [
//                [
//                    'AttributeName' => 'id',
//                    'KeyType' => 'HASH',
//                ]
//            ],
//            'ProvisionedThroughput' => [
//                'ReadCapacityUnits' => 10,
//                'WriteCapacityUnits' => 10
//            ]
//        ]);
//        $objectToInsert = new data\Customer();
//        $id = \Faker\Provider\Base::randomNumber(5);
//        $faker = \Faker\Factory::create();
//        $objectToInsert->id = $id;
//        $objectToInsert->name = $faker->name;
//        $objectToInsert->contacts = [
//            'telephone1' => 123456,
//            'telephone2' => 345678,
//            'telephone3' => 345678,
//        ];
//        $objectToInsert->prices = [
//            1000000,
//            1000000,
//            1000000,
//            1000000
//        ];
//        $objectToInsert->kids = [
//            'Alice',
//            'Billy',
//            'Charlie',
//        ];
//        
////        $this->assertTrue($objectToInsert->save(false));
////        
//        $objectFromFind = data\Customer::findOne($id);
////        print_r($objectFromFind);
//        /* @var $objectFromFind data\Customer */
////        $this->assertNotNull($objectFromFind);
////        $this->assertEquals($id, $objectFromFind->id);
////        $this->assertEquals($objectToInsert->name, $objectFromFind->name);
//    }
}
