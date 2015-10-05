<?php

class ConnectionTest extends TestCase {
    
    public function testConnectionClient() {
        $component = new \UrbanIndo\Yii2\DynamoDb\Connection([
            'config' => [
                'credentials' => [
                    'key' => 'AKIA1234567890',
                    'secret' => '1234567890',
                ],
                'region' => 'ap-southeast-1',
                'version' => 'latest',
                'endpoint' => DYNAMODB_URL,
            ]
        ]);
        $client = $component->getClient();
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
}
