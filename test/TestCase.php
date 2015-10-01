<?php

abstract class TestCase extends \PHPUnit_Framework_TestCase {
     
    protected function setUp() {
        parent::setUp();
        $command = $this->getConnection()->createCommand();
        /* @var $command ClassName */
        if (!$command->tableExists(\test\data\Customer::tableName())) {
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
        }
    }
    
    /**
     * @return \UrbanIndo\Yii2\DynamoDb\Connection
     */
    public function getConnection() {
        return Yii::$app->dynamodb;
    }
}
