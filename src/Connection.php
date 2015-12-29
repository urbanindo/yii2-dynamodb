<?php
/**
 * Connection class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use Aws\DynamoDb\DynamoDbClient;

/**
 * Connection wraps DynamoDB connection for Aws PHP SDK.
 *
 * To use the connection puts this in the config.
 *
 * ```
 * ```
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Connection extends \yii\base\Component
{
    
    /**
     * @var array the configuration for DynamoDB client.
     */
    public $config;
    
    /**
     * @var DynamoDbClient the DynamoDB client.
     */
    protected $_client;
    protected $_builder;
    
    /**
     * Initialize the dynamodb client.
     */
    public function init()
    {
        parent::init();
        //For v2 compatibility.
        //TODO: remove deprecated.
        $this->_client = DynamoDbClient::factory($this->config);
    }
    
    /**
     * @return DynamoDbClient
     */
    public function getClient()
    {
        return $this->_client;
    }
    
    /**
     * Creates a command for execution.
     * @param array $config the configuration for the Command class
     * @return Command the DB command
     */
    public function createCommand($config = [])
    {
        $command = Yii::createObject(array_merge($config, [
            'class' => Command::className(),
            'db' => $this
        ]));
        return $command;
    }

    /**
     * @return QueryBuilder
     */
    public function getQueryBuilder()
    {
        if ($this->_builder === null) {
            $this->_builder = new QueryBuilder($this);
        }
        return $this->_builder;

    }
}
