<?php

class ConnectionTest extends PHPUnit_Framework_TestCase {
    
    public function testConnectionClient() {
        $component = new \UrbanIndo\Yii2\DynamoDb\Connection([
            'config' => [
                'profile' => 'default',
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
        $client->execute($command);
    }
}
