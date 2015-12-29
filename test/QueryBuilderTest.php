<?php

use UrbanIndo\Yii2\DynamoDb\Query;

class QueryBuilderTest extends TestCase
{
    
    /**
     * @return \UrbanIndo\Yii2\DynamoDb\QueryBuilder
     */
    private function getQueryBuilder()
    {
        $connection = $this->getConnection();
        return $connection->getQueryBuilder();
    }
    
    public function testCreateTable()
    {
        $qb = $this->getQueryBuilder();
        list($name, $options) = $qb->createTable('Test');
        $this->assertEquals('CreateTable', $name);
        $this->assertEquals([
            'TableName' => 'Test'
        ], $options);
    }
}
