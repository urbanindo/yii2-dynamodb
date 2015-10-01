<?php

class CommandTest extends TestCase {
    
    public function testCreate() {
        /* @var $client \Aws\DynamoDb\DynamoDbClient */
        $client = Yii::$app->dynamodb->getClient();
        $command = $client->getCommand('CreateTable', [
            'TableName' => 'Testing',
            'KeySchema' => [
                [
                    'AttributeName' => 'Test1',
                    'KeyType' => 'HASH',
                ]
            ],
            'AttributeDefinitions' => [
                [
                    'AttributeName' => 'Test1',
                    'AttributeType' => 'S',
                ]
            ],
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => 5,
                'WriteCapacityUnits' => 5,
            ]
        ]);
        $result = $client->execute($command);
        /* @var $result Aws\Result */
        $description = $result->get('TableDescription');
        $this->assertNotEmpty($description);
    }
    public function testPut() {
        /* @var $client \Aws\DynamoDb\DynamoDbClient */
        $client = Yii::$app->dynamodb->getClient();
        
        $marshaler = new \UrbanIndo\Yii2\DynamoDb\Marshaler();
        $command = $client->getCommand('PutItem', [
            'TableName' => 'Testing',
            "Item" => $marshaler->marshalItem([
                'Test1' => 'key',
                "testobj1" => [
                    'ada' => [
                        'arr' => ["a", "b"],
                        'p' => 'x'
                    ]
                ],
            ])
        ]);
        /* @var $result \Guzzle\Service\Resource\Model */
        $result = $command->execute();
        $this->assertNotNull($result);
    }
    public function testGet() {
        /* @var $client \Aws\DynamoDb\DynamoDbClient */
        $client = Yii::$app->dynamodb->getClient();
        $command = $client->getCommand('GetItem', [
            'TableName' => 'Testing',
            "Key" => [
                'Test1' => [
                    'S' => 'key'
                ],
            ]
        ]);
        /* @var $result \Guzzle\Service\Resource\Model */
        $result = $command->execute();
        $result = (new \UrbanIndo\Yii2\DynamoDb\Marshaler)->unmarshalItem($result->get('Item'));
        $this->assertArraySubset(['Test1' => 'key'], $result);
    }
}

