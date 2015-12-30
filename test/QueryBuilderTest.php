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
        $index1 = Customer::secondaryIndex()[0];
        $indexFieldName1 = Customer::keySecondayIndex()[$index1][0];

        if (!$command->tableExists($tableName)) {
            $command->createTable($tableName, [
                'KeySchema' => [
                    [
                        'AttributeName' => $fieldName1,
                        'KeyType' => 'HASH',
                    ],
                ],
                'AttributeDefinitions' => [
                    [
                        'AttributeName' => $fieldName1,
                        'AttributeType' => 'S',
                    ],
                    [
                        'AttributeName' => $indexFieldName1,
                        'AttributeType' => 'S',
                    ],
                ],
                'LocalSecondaryIndexes' => [
                    [
                        'IndexName' => $index1,
                        'KeySchema' => [
                            [ 'AttributeName' => $index1, 'KeyType' => 'HASH' ]
                        ],
                        'Projection' => [
                            'ProjectionType' => 'KEYS_ONLY'
                        ],
                    ],
                ],
                'ProvisionedThroughput' => [
                    'ReadCapacityUnits' => 5,
                    'WriteCapacityUnits' => 5,
                ],
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
        $query2 = $this->createQueryGetItem()->where($id);
        $query3 = $this->createQueryGetItem()->where($id)->withConsistentRead();
        $query4 = $this->createQueryGetItem()->where($id)->withoutConsistentRead();
        $query5 = $this->createQueryGetItem()->where($id)->setConsumedCapacity('NONE');

        $expected = [
            'TableName' => Customer::tableName(),
            'Key' => [
                'id' => ['S' => $id]
            ]
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
        $this->assertEquals($expected, $qb->build($query2)[1]);

        $expected['ConsistentRead'] = true;
        $this->assertEquals($expected, $qb->build($query3)[1]);
        $expected['ConsistentRead'] = false;
        $this->assertEquals($expected, $qb->build($query4)[1]);
        unset($expected['ConsistentRead']);
        $expected['ReturnConsumedCapacity'] = 'NONE';
        $this->assertEquals($expected, $qb->build($query5)[1]);
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

        $expected = [
            'TableName' => Customer::tableName(),
            'Key' => [
                'id' => ['S' => $id]
            ],
            'ProjectionExpression' => 'id, name, contacts',
        ];

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
        $id = $faker->firstNameFemale;
        $query1 = $this->createQueryGetBatchItem()->where(['id' => $id]);
        $query2 = $this->createQueryGetBatchItem()->where($id);
        $query3 = $this->createQueryGetBatchItem()->where($id)->withConsistentRead();
        $query4 = $this->createQueryGetBatchItem()->where($id)->withoutConsistentRead();
        $query5 = $this->createQueryGetBatchItem()->where($id)->setConsumedCapacity('NONE');

        $expected = [
            'RequestItems' => [
                Customer::tableName() => [
                    'Keys' => [
                        [
                            'id' => ['S' => $id]
                        ]
                    ],
                ]
            ]
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
        $this->assertEquals($expected, $qb->build($query2)[1]);

        $expected['RequestItems'][Customer::tableName()]['ConsistentRead'] = true;
        $this->assertEquals($expected, $qb->build($query3)[1]);
        $expected['RequestItems'][Customer::tableName()]['ConsistentRead'] = false;
        $this->assertEquals($expected, $qb->build($query4)[1]);
        unset($expected['RequestItems'][Customer::tableName()]['ConsistentRead']);
        $expected['RequestItems'][Customer::tableName()]['ReturnConsumedCapacity'] = 'NONE';
        $this->assertEquals($expected, $qb->build($query5)[1]);
    }

    /**
     * Test build simple GetBatchItem method
     * @return void
     */
    public function testBuildSimpleGetBatchItem2()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $id3 = $faker->firstNameFemale;
        $query1 = $this->createQueryGetBatchItem()->where(['IN', 'id', [$id1, $id2, $id3]]);
        $query2 = $this->createQueryGetBatchItem()->where(['id' => [$id1, $id2, $id3]]);

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
                    ]
                ]
            ]
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
        $this->assertEquals($expected, $qb->build($query2)[1]);
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
                    'ProjectionExpression' => 'id, name, contacts',
                ]
            ]
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Simple Scan method with no parameter
     * @return void
     */
    public function testBuildSimpleScanNoParameter()
    {
        $qb = $this->createQueryBuilder();
        $query1 = $this->createQueryScan();

        $expected = [
            'TableName' => Customer::tableName(),
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Simple Scan method
     * @return void
     */
    public function testBuildSimpleScan()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQueryScan()->where(['id' => $id]);
        $query2 = $this->createQueryScan()->where(['id' => $id])->withConsistentRead();
        $query3 = $this->createQueryScan()->where(['id' => $id])->withoutConsistentRead();
        $query4 = $this->createQueryScan()->where(['id' => $id])->setConsumedCapacity('NONE');

        $expected = [
            'TableName' => Customer::tableName(),
            'FilterExpression' => 'id=:dqp0',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $id]
            ],
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);

        $expected['ConsistentRead'] = true;
        $this->assertEquals($expected, $qb->build($query2)[1]);
        $expected['ConsistentRead'] = false;
        $this->assertEquals($expected, $qb->build($query3)[1]);
        unset($expected['ConsistentRead']);
        $expected['ReturnConsumedCapacity'] = 'NONE';
        $this->assertEquals($expected, $qb->build($query4)[1]);
    }

    /**
     * Test build Scan method with simple select
     * @return void
     */
    public function testBuildScanWithSimpleSelect()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['id' => $id]);

        $expected = [
            'TableName' => Customer::tableName(),
            'FilterExpression' => 'id=:dqp0',
            'ProjectionExpression' => 'id, name, contacts',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $id]
            ],
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Scan method with simple select
     * @return void
     */
    public function testBuildScanWithIndex()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['name' => $id])->indexBy(Customer::secondaryIndex()[0]);

        $expected = [
            'TableName' => Customer::tableName(),
            'IndexName' => Customer::secondaryIndex()[0],
            'FilterExpression' => 'name=:dqp0',
            'ProjectionExpression' => 'id, name, contacts',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $id]
            ],
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Scan method with complicated condition
     * @return void
     */
    public function testBuildScanWithComplicatedCondition()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $name = $faker->firstNameFemale;
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $query1 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['id' => $id1]);
        $query2 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['id' => [$id1, $id2]]);
        $query3 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['attribute_exists', 'id']);
        $query4 = $this->createQueryScan()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['begins_with', 'id', $id1]);

        $expected = [
            'TableName' => Customer::tableName(),
            'FilterExpression' => '(name=:dqp0) AND (id=:dqp1)',
            'ProjectionExpression' => 'id, name, contacts',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $name],
                ':dqp1' => ['S' => $id1]
            ],
        ];
        $this->assertEquals($expected, $qb->build($query1)[1]);

        $expected['FilterExpression'] = '(name=:dqp0) AND (id IN (:dqp1, :dqp2))';
        $expected['ExpressionAttributeValues'][':dqp2'] = ['S' => $id2];
        $this->assertEquals($expected, $qb->build($query2)[1]);

        $expected['FilterExpression'] = '(name=:dqp0) AND (attribute_exists (:dqp1))';
        $expected['ExpressionAttributeValues'][':dqp1'] = ['S' => 'id'];
        unset($expected['ExpressionAttributeValues'][':dqp2']);
        $this->assertEquals($expected, $qb->build($query3)[1]);

        $expected['FilterExpression'] = '(name=:dqp0) AND (begins_with (:dqp1, :dqp2))';
        $expected['ExpressionAttributeValues'][':dqp2'] = ['S' => $id1];
        $this->assertEquals($expected, $qb->build($query4)[1]);
    }

    /**
     * Test build Simple Scan method with no parameter
     * @return void
     */
    public function testBuildSimpleQueryNoParameter()
    {
        $qb = $this->createQueryBuilder();
        $query1 = $this->createQuery();

        $expected = [
            'TableName' => Customer::tableName(),
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Simple Query method
     * @return void
     */
    public function testBuildSimpleQuery()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQuery()->where(['id' => $id]);
        $query2 = $this->createQuery()->where(['id' => $id])->withConsistentRead();
        $query3 = $this->createQuery()->where(['id' => $id])->withoutConsistentRead();
        $query4 = $this->createQuery()->where(['id' => $id])->setConsumedCapacity('NONE');

        $expected = [
            'TableName' => Customer::tableName(),
            'KeyConditionExpression' => 'id=:dqp0',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $id]
            ],
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);

        $expected['ConsistentRead'] = true;
        $this->assertEquals($expected, $qb->build($query2)[1]);
        $expected['ConsistentRead'] = false;
        $this->assertEquals($expected, $qb->build($query3)[1]);
        unset($expected['ConsistentRead']);
        $expected['ReturnConsumedCapacity'] = 'NONE';
        $this->assertEquals($expected, $qb->build($query4)[1]);
    }

    /**
     * Test build Query method with simple select
     * @return void
     */
    public function testBuildQueryWithSimpleSelect()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQuery()->select(['id', 'name', 'contacts'])
            ->where(['id' => $id]);

        $expected = [
            'TableName' => Customer::tableName(),
            'KeyConditionExpression' => 'id=:dqp0',
            'ProjectionExpression' => 'id, name, contacts',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $id]
            ],
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Query method with index parameter
     * @return void
     */
    public function testBuildQueryWithIndex()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $id = $faker->firstNameFemale;
        $query1 = $this->createQuery()->select(['id', 'name', 'contacts'])
            ->where(['name' => $id])->indexBy(Customer::secondaryIndex()[0]);

        $expected = [
            'TableName' => Customer::tableName(),
            'IndexName' => Customer::secondaryIndex()[0],
            'KeyConditionExpression' => 'name=:dqp0',
            'ProjectionExpression' => 'id, name, contacts',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $id]
            ],
        ];

        $this->assertEquals($expected, $qb->build($query1)[1]);
    }

    /**
     * Test build Query method with complicated condition
     * @return void
     */
    public function testBuildQueryWithComplicatedCondition()
    {
        $qb = $this->createQueryBuilder();
        $faker = \Faker\Factory::create();
        $name = $faker->firstNameFemale;
        $id1 = $faker->firstNameFemale;
        $id2 = $faker->firstNameFemale;
        $query1 = $this->createQuery()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['id' => $id1]);
        $query2 = $this->createQuery()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['id' => [$id1, $id2]]);
        $query3 = $this->createQuery()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['attribute_exists', 'id']);
        $query4 = $this->createQuery()->select(['id', 'name', 'contacts'])
            ->where(['name' => $name])->andWhere(['begins_with', 'id', $id1]);

        $expected = [
            'TableName' => Customer::tableName(),
            'KeyConditionExpression' => '(name=:dqp0) AND (id=:dqp1)',
            'ProjectionExpression' => 'id, name, contacts',
            'ExpressionAttributeValues' => [
                ':dqp0' => ['S' => $name],
                ':dqp1' => ['S' => $id1]
            ],
        ];
        $this->assertEquals($expected, $qb->build($query1)[1]);

        $expected['KeyConditionExpression'] = '(name=:dqp0) AND (id IN (:dqp1, :dqp2))';
        $expected['ExpressionAttributeValues'][':dqp2'] = ['S' => $id2];
        $this->assertEquals($expected, $qb->build($query2)[1]);

        $expected['KeyConditionExpression'] = '(name=:dqp0) AND (attribute_exists (:dqp1))';
        $expected['ExpressionAttributeValues'][':dqp1'] = ['S' => 'id'];
        unset($expected['ExpressionAttributeValues'][':dqp2']);
        $this->assertEquals($expected, $qb->build($query3)[1]);

        $expected['KeyConditionExpression'] = '(name=:dqp0) AND (begins_with (:dqp1, :dqp2))';
        $expected['ExpressionAttributeValues'][':dqp2'] = ['S' => $id1];
        $this->assertEquals($expected, $qb->build($query4)[1]);
    }
}
