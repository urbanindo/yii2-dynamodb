<?php
/**
 * PHP Unit Test Module for Query Builder
 *
 * @author Setyo Legowo <setyo@urbandindo.com>
 */

use UrbanIndo\Yii2\DynamoDb\Query;
use UrbanIndo\Yii2\DynamoDb\QueryBuilder;
use test\data\Customer;

/**
 * PHP Unit Test Class for Query Builder
 *
 * @author Setyo Legowo <setyo@urbanindo.com>
 */
class QueryBuilderTest extends TestCase
{

    public $use_backward_compatible = false;

    /**
     * @var Connection the database connection.
     */
    public $db;

    /**
     * Initiate testing
     * @return void
     */
    public function setUp()
    {
        $this->db = $this->getConnection();
        $command = $this->db->createCommand();
        $faker = \Faker\Factory::create();
        $tableName = Customer::tableName();
        $fieldName1 = Customer::primaryKey()[0];

        if (!$command->tableExists($tableName)) {
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
    }

    /**
     * Get Query Builder
     * @return QueryBuilder
     */
    public function createQueryBuilder()
    {
        return new QueryBuilder($this->db);
    }

    /**
     * Get Query Get Item method
     * @return Query
     */
    public function createQueryGetItem()
    {
        $query = new Query();
        $query->using = Query::USING_GET_ITEM;
        $query->from(Customer::tableName());

        return $query;
    }

    /**
     * Get Query Get Batch Item method
     * @return Query
     */
    public function createQueryGetBatchItem()
    {
        $query = new Query();
        $query->using = Query::USING_BATCH_GET_ITEM;
        $query->from(Customer::tableName());

        return $query;
    }

    /**
     * Get Query Scan method
     * @return Query
     */
    public function createQueryScan()
    {
        $query = new Query();
        $query->using = Query::USING_SCAN;
        $query->from(Customer::tableName());

        return $query;
    }

    /**
     * Get Query method
     * @return Query
     */
    public function createQuery()
    {
        $query = new Query();
        $query->using = Query::USING_QUERY;
        $query->from(Customer::tableName());

        return $query;
    }

    /**
     * Test build simple GetItem method
     * @return void
     */
    public function testBuildSimpleGetItem()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQueryGetItem()->where(['id' => $id]);

        $expected = [
            'TableName' => Customer::tableName(),
            'Key' => [
                'id' => ['S' => $id]
            ],
            'ConsistentRead' => false,
            'ReturnConsumedCapacity' => false,
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build GetItem method with simple select
     * @return void
     */
    public function testBuildGetItemWithSimpleSelect()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQueryGetItem()->select(['id', 'name', 'contacts'])
            ->where(['id' => $id]);

        if ($this->use_backward_compatible) {
            $expected = [
                'TableName' => Customer::tableName(),
                'Key' => [
                    'id' => ['S' => $id]
                ],
                'ConsistentRead' => false,
                'ReturnConsumedCapacity' => false,
                'AttributesToGet' => ['id', 'name', 'contacts'],
            ];
        } else {
            $expected = [
                'TableName' => Customer::tableName(),
                'Key' => [
                    'id' => ['S' => $id]
                ],
                'ConsistentRead' => false,
                'ReturnConsumedCapacity' => false,
                'ProjectionExpression' => 'id, name, contacts',
            ];
        }

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build simple GetBatchItem method
     * @return void
     */
    public function testBuildSimpleGetBatchItem()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $id3 = $faker->firstNameFemale;
        $query1 = $this->createQueryGetBatchItem()->where(['IN', 'id', [$id1, $id2, $id3]]);

        $expected = [
            'RequestItems' => [
                Customer::tableName() => [
                    'Keys' => [
                        [
                            'id' => ['S' => $id1]
                        ],
                        [
                            'id' => ['S' => $id2]
                        ],
                        [
                            'id' => ['S' => $id3]
                        ]
                    ],
                    'ConsistentRead' => false,
                    'ReturnConsumedCapacity' => false,
                ]
            ]
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Get Batch Item method with simple select
     * @return void
     */
    public function testBuildGetBatchItemWithSimpleSelect()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $id3 = $faker->firstNameFemale;
        $query1 = $this->createQueryGetBatchItem()->select(['id', 'name', 'contacts'])
            ->where(['IN', 'id', [$id1, $id2, $id3]]);

        if ($this->use_backward_compatible) {
            $expected = [
                'RequestItems' => [
                    Customer::tableName() => [
                        'Keys' => [
                            [
                                'id' => ['S' => $id1]
                            ],
                            [
                                'id' => ['S' => $id2]
                            ],
                            [
                                'id' => ['S' => $id3]
                            ]
                        ],
                        'ConsistentRead' => false,
                        'ReturnConsumedCapacity' => false,
                        'AttributesToGet' => ['id', 'name', 'contacts'],
                    ]
                ]
            ];
        } else {
            $expected = [
                'RequestItems' => [
                    Customer::tableName() => [
                        'Keys' => [
                            [
                                'id' => ['S' => $id1]
                            ],
                            [
                                'id' => ['S' => $id2]
                            ],
                            [
                                'id' => ['S' => $id3]
                            ]
                        ],
                        'ConsistentRead' => false,
                        'ReturnConsumedCapacity' => false,
                        'ProjectionExpression' => 'id, name, contacts',
                    ]
                ]
            ];
        }

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Simple Scan method
     * @return void
     */
    public function _testBuildSimpleScan()
    {
        $qb = $this->createQueryBuilder();
        $query1 = $this->createQueryScan()->where(['id' => 1]);

        if ($this->use_backward_compatible) {
            $expected = [
                'TableName' => Customer::tableName(),
                'ScanFilter' => [
                    'id' => [
                        'AttributeValueList' => [
                            ['N' => '1']
                        ],
                        'ComparisonOperator' => 'EQ'
                    ]
                ],
                'ConsistentRead' => false,
            ];
        } else {
            $expected = [
                'TableName' => Customer::tableName(),
                'FilterExpression' => 'id = :val1',
                'ExpressionAttributeValues' => [
                    ':val1' => ['N' => '1']
                ],
                'ConsistentRead' => false,
            ];
        }

        $this->assertEquals($expected, $qb->build($query1));
    }

    /**
     * Test build Scan method with simple select
     * @return void
     */
    public function _testBuildScanWithSimpleSelect()
    {
        $qb = $this->createQueryBuilder();
        $query1 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['id' => 1]);

        if ($this->use_backward_compatible) {
            $expected = [
                'TableName' => Customer::tableName(),
                'AttributesToGet' => ['id', 'name', 'contacts'],
                'ScanFilter' => [
                    'id' => [
                        'AttributeValueList' => [
                            ['N' => '1']
                        ],
                        'ComparisonOperator' => 'EQ'
                    ]
                ],
                'ConsistentRead' => false,
            ];
        } else {
            $expected = [
                'TableName' => Customer::tableName(),
                'FilterExpression' => 'id = :val1',
                'ProjectionExpression' => 'id, name, contacts',
                'ExpressionAttributeValues' => [
                    ':val1' => ['N' => '1']
                ],
                'ConsistentRead' => false,
            ];
        }

        $this->assertEquals($expected, $qb->build($query1));
    }

    /**
     * Test build Simple Query method
     * @return void
     */
    public function _testBuildSimpleQuery()
    {
        $qb = $this->createQueryBuilder();
        $query1 = $this->createQuery()->where(['id' => 1]);

        if ($this->use_backward_compatible) {
            $expected = [
                'TableName' => Customer::tableName(),
                'KeyConditions' => [
                    'id' => [
                        'AttributeValueList' => [
                            ['N' => '1']
                        ],
                        'ComparisonOperator' => 'EQ'
                    ]
                ],
                'ConsistentRead' => false,
            ];
        } else {
            $expected = [
                'TableName' => Customer::tableName(),
                'KeyConditionExpression' => 'id = :val1',
                'ExpressionAttributeValues' => [
                    ':val1' => ['N' => '1']
                ],
                'ConsistentRead' => false,
            ];
        }

        $this->assertEquals($expected, $qb->build($query1));
    }

    /**
     * Test build Query method with simple select
     * @return void
     */
    public function _testBuildQueryWithSimpleSelect()
    {
        $qb = $this->createQueryBuilder();
        $query1 = $this->createQuery()->where(['id' => 1]);

        if ($this->use_backward_compatible) {
            $expected = [
                'TableName' => Customer::tableName(),
                'AttributesToGet' => ['id', 'name', 'contacts'],
                'KeyConditions' => [
                    'id' => [
                        'AttributeValueList' => [
                            ['N' => '1']
                        ],
                        'ComparisonOperator' => 'EQ'
                    ]
                ],
                'ConsistentRead' => false,
            ];
        } else {
            $expected = [
                'TableName' => Customer::tableName(),
                'KeyConditionExpression' => 'id = :val1',
                'ProjectionExpression' => 'id, name, contacts',
                'ExpressionAttributeValues' => [
                    ':val1' => ['N' => '1']
                ],
                'ConsistentRead' => false,
            ];
        }

        $this->assertEquals($expected, $qb->build($query1));
    }
}
