<?php

use yii\helpers\ArrayHelper;

abstract class TestCase extends \PHPUnit_Framework_TestCase
{

    /**
     * @return \UrbanIndo\Yii2\DynamoDb\Connection
     */
    public function getConnection()
    {
        $this->mockWebApplication([
            'components' => [
                'dynamodb' => [
                    /* @var $dynamodb \UrbanIndo\Yii2\DynamoDb\Connection */
                    'class' => '\UrbanIndo\Yii2\DynamoDb\Connection',
                    'config' => [
                        'credentials' => [
                            'key' => 'AKIA',
                            'secret' => '1234567890',
                        ],
                        'region' => 'ap-southeast-1',
                        'version' => 'latest',
                        'endpoint' => DYNAMODB_URL,
                    ]
                ]
            ]
        ]);
        return Yii::$app->dynamodb;
    }

    /**
     * @return \UrbanIndo\Yii2\DynamoDb\Command
     */
    public function createCommand()
    {
        return $this->getConnection()->createCommand();
    }

    public function getTableItemCount($tableName) {
        $tableDescription = $this->getConnection()->createCommand()->describeTable($tableName)->execute();
        return $tableDescription['Table']['ItemCount'];
    }


    public function createSimpleTableWithHashKey()
    {
        $command = $this->createCommand();
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

        return [$tableName, $fieldName1];
    }

    public function createSimpleTableWithHashKeyAndRangeKey()
    {
        $command = $this->createCommand();
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

        return [$tableName, $fieldName1, $fieldName2];
    }

    protected function mockWebApplication($config = [], $appClass = '\yii\web\Application')
    {
        new $appClass(ArrayHelper::merge([
            'id' => 'testapp',
            'basePath' => __DIR__,
            'vendorPath' => $this->getVendorPath(),
            'components' => [
                'request' => [
                    'cookieValidationKey' => 'wefJDF8sfdsfSDefwqdxj9oq',
                    'scriptFile' => __DIR__ .'/index.php',
                    'scriptUrl' => '/index.php',
                ],
            ]
        ], $config));
    }

    protected function getVendorPath()
    {
        $vendor = dirname(dirname(__DIR__)) . '/vendor';
        if (!is_dir($vendor)) {
            $vendor = dirname(dirname(dirname(dirname(__DIR__))));
        }
        return $vendor;
    }

    protected function createCustomersTable()
    {
        $command = $this->createCommand();
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

    protected function createCustomersRangeTable()
    {
        $command = $this->createCommand();
        /* @var $command \UrbanIndo\Yii2\DynamoDb\Command */
        $table = \test\data\CustomerRange::tableName();
        if ($command->tableExists($table)) {
            $command->deleteTable($table)->execute();
        }
        $index = \test\data\CustomerRange::secondaryIndex()[0];
        $command->createTable($table, [
            'AttributeDefinitions' => [
                [
                    'AttributeName' => \test\data\CustomerRange::primaryKey()[0],
                    'AttributeType' => 'N'
                ],
                [
                    'AttributeName' => \test\data\CustomerRange::primaryKey()[1],
                    'AttributeType' => 'S'
                ],
                [
                    'AttributeName' => \test\data\CustomerRange::keySecondayIndex()[$index][1],
                    'AttributeType' => 'S'
                ]
            ],
            'KeySchema' => [
                [
                    'AttributeName' => \test\data\CustomerRange::primaryKey()[0],
                    'KeyType' => 'HASH',
                ],
                [
                    'AttributeName' => \test\data\CustomerRange::primaryKey()[1],
                    'KeyType' => 'RANGE',
                ]
            ],
            'LocalSecondaryIndexes' => [
                [
                    'IndexName' => $index,
                    'KeySchema' => [
                        [
                            'AttributeName' => \test\data\CustomerRange::keySecondayIndex()[$index][0],
                            'KeyType' => 'HASH',
                        ],
                        [
                            'AttributeName' => \test\data\CustomerRange::keySecondayIndex()[$index][1],
                            'KeyType' => 'RANGE',
                        ]
                    ],
                    'Projection' => [
                        'ProjectionType' => 'ALL',
                    ]
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 10,
                'WriteCapacityUnits' => 10
            ]
        ])->execute();
    }
}
