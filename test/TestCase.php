<?php

abstract class TestCase extends \PHPUnit_Framework_TestCase
{
    
    /**
     * @return \UrbanIndo\Yii2\DynamoDb\Connection
     */
    public function getConnection()
    {
        return Yii::$app->dynamodb;
    }
}
