<?php
/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
namespace UrbanIndo\Yii2\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Guzzle\Service\Resource\Model;
use Yii;
use yii\base\NotSupportedException;
use yii\base\Object;

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Command extends Object {
    
    /**
     * @var Connection
     */
    public $db;
    public $method;
    public $request;

    public function init() {
    }
    /**
     * @return DynamoDbClient
     */
    protected function getClient() {
        return $this->db->getClient();
    }

    /**
     * 
     * @return Model
     */
    public function execute() {
        Yii::info("{$this->method}: " . json_encode($this->request) , 'yii\db\Command::query');
        $command = $this->getClient()->getCommand($this->method, $this->request);
        return $this->getClient()->execute($command);
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
        return $this->putItem($tableName, $values);
    }
    
    /**
     * @param string $tableName
     * @param array $values
     */
    public function putItem($tableName, $values) {
        $marshaler = new Marshaler();
        $command = $this->getClient()->getCommand('PutItem', [
            'TableName' => $tableName,
            'Item' => $marshaler->marshalItem($values),
        ]);
        $marshaler->marshalItem($values);
        try {
            $this->getClient()->execute($command);
            return true;
        } catch (\Exception $exc) {
            return false;
        }
    }
    /**
     * @param string $tableName
     * @param array $values
     */
    public function putItems($tableName, $values) {
        $unprocessedItems = [];
        foreach ($values as $item) {
            $unprocessedItems[] = [
                'PutRequest' => [
                    'Item' => Marshaler::marshal($item)
                ]
            ];
                    
        }
        while (!empty($unprocessedItems)) {
            $chunks = array_chunk($unprocessedItems, 25);
            $unprocessedItems = [];
            foreach ($chunks as $chunk) {
                $request = [
                    'RequestItems' => [
                        $tableName => $chunk
                    ]
                ];
                $command = $this->getClient()->getCommand('BatchWriteItem', $request);
                $response = $this->getClient()->execute($command);
                if (isset($response->get("UnprocessedItems")[$tableName])) {
                    $unprocessedItems = $unprocessedItems + $response->get("UnprocessedItems")[$tableName];
                }
            }
        }
        return $response;
    }
    public function count() {
        if ($this->method == Query::TYPE_GET) {
            return 1;
        } else if ($this->method == Query::TYPE_BATCH_GET) {
            $tables = array_keys($this->request['RequestItems']);
            $count = 0;
            foreach ($tables as $keys) {
                $count += count(reset($keys));
            }
            return $count;
        } else {
            // TODO use query Select => COUNT
            throw new NotSupportedException('Not implemented yet');
        }
    }

    /**
     * Increase can be done unlimited time, decrease max 4 times a day
     * @param string $tablename
     * @param int $readThroughput
     * @param int $writeThroughput
     * @return array @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html
     */
    public function updateThroughput($tablename, $readThroughput, $writeThroughput) {
        $request = [
            'TableName' => $tablename,
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => $readThroughput,
                'WriteCapacityUnits' => $writeThroughput
            ]
        ];
        $command = $this->getClient()->getCommand('UpdateTable', $request);
        return $this->getClient()->execute($command);
    }
    /**
     * @param type $tableName
     * @return array @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html
     */
    public function describeTable($tableName) {
        $command = $this->getClient()->getCommand('DescribeTable', [
                'TableName' => $tableName,
            ]);
        return $this->getClient()->execute($command)->get('Table');
    }
    
    /**
     * Return whether a table exists or not.
     * @param string $tableName the name of the table.
     * @return boolean
     */
    public function tableExists($tableName) {
        try {
            $this->describeTable($tableName);
            return true;
        } catch (\Aws\DynamoDb\Exception\ResourceNotFoundException $exc) {
            return false;
        }
    }
    
    /**
     * 
     * @return type
     */
    public function queryOne() {
        $response = $this->execute();
        return $response;
    }
    public function queryAll() {
        $response = $this->execute();
        return $response->get('Responses');
    }
}
