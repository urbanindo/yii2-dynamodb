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
    public $method;
    public $request;

    public function init() {
    }
    /**
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    protected function getClient() {
        return $this->db->getClient();
    }

    /**
     * 
     * @return \Guzzle\Service\Resource\Model
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
        $this->putItem($tableName, $values);
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
        return $this->getClient()->execute($command);
    }
    /**
     * @param string $tableName
     * @param array $values
     */
    public function putItems($tableName, $values) {
        $marshaler = new Marshaler();
        $chunks = array_chunk($values, 25);
        $response = [];
        foreach ($chunks as $chunk) {
            $request = [
                'RequestItems' => [
                    $tableName => []
                ]
            ];
            foreach ($chunk as $item) {
                $request['RequestItems'][$tableName][] = [
                    'PutRequest' => [
                        'Item' => $marshaler->marshalItem($item)
                    ]
                ];
            }
            $command = $this->getClient()->getCommand('BatchWriteItem', $request);
            $response[] = $this->getClient()->execute($command);
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
            throw new \yii\base\NotSupportedException('Not implemented yet');
        }
    }
    
    public function queryOne() {
        $response = $this->execute();
        return $response->get('Item');
    }
    public function queryAll() {
        $response = $this->execute();
        print_r($response);
        return $response->get('Responses');
    }
}
