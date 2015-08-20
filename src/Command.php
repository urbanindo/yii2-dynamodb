<?php
/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
namespace UrbanIndo\Yii2\DynamoDb;

use Aws\DynamoDb\Marshaler;

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Command extends \yii\base\Object {
    
    /**
     * @var Connection
     */
    public $db;
    
    /**
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    protected function getClient() {
        return $this->db->getClient();
    }


    /**
     * Create new table.
     * @param string $tableName the name of the table.
     * @param array $options valid options for `CreateTable` command.
     */
    public function createTable($tableName, $options) {
        $command = $this->getClient()->getCommand('CreateTable', array_merge([
                'TableName' => $tableName,
            ],
            $options));
        return $this->getClient()->execute($command);
    }
    
    /**
     * @param string $tableName
     * @param array $values
     */
    public function insert($tableName, $values) {
        $marshaler = new Marshaler();
        $command = $this->getClient()->getCommand('PutItem', [
            'TableName' => $tableName,
            'Item' => $marshaler->marshalItem($values),
        ]);
        return $this->getClient()->execute($command);
    }
    
}
