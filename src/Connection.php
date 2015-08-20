<?php
/**
 * Connection class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */
namespace UrbanIndo\Yii2\DynamoDb;

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
class Connection extends \yii\base\Component {
    
    /**
     * @var array the configuration for DynamoDB client.
     */
    public $config;
    
    /**
     * @var DynamoDbClient the DynamoDB client.
     */
    protected $_client;
    
    /**
     * Initialize the dynamodb client.
     */
    public function init() {
        parent::init();
        //For v2 compatibility.
        //TODO: remove deprecated.
        $this->_client = DynamoDbClient::factory($this->config);
    }
    
    /**
     * @return DynamoDbClient
     */
    public function getClient() {
        return $this->_client;
    }
}
