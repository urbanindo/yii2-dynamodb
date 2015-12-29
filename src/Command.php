<?php
/**
 * Command class file.
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
class Command extends Object
{

    /**
     * @var Connection
     */
    public $db;
    
    /**
     * The name of the DynamoDB request. For example `CreateTable`, `GetItem`.
     * @var string
     */
    public $name;
    
    /**
     * The argument of the DynamoDB. This contains, for example `KeySchema`,
     * `AttributeDefinitions`, etc.
     * @var array
     */
    public $argument;

    /**
     * @return DynamoDbClient
     */
    protected function getClient()
    {
        return $this->db->getClient();
    }

    /**
     * Execute the command.
     * @return array The array result of the command execution.
     */
    public function execute()
    {
        Yii::info("{$this->name}: " . json_encode($this->argument), '\UrbanIndo\Yii2\DynamoDb::execute');
        $command = $this->getClient()->getCommand($this->name, $this->argument);
        $result = $this->getClient()->execute($command);
        /* @var $result \Guzzle\Service\Resource\Model */
        return $result->toArray();
    }
    
    /**
     * Specifies the command and the argument to be requested to DynamoDB.
     * @param string $name     The command name.
     * @param array  $argument The command argument.
     * @return static
     */
    public function setCommand($name, array $argument) {
        $this->name = $name;
        $this->argument = $argument;
        return $this;
    }

    /**
     * Create new table.
     * @param string $table the name of the table.
     * @param array $options valid options for `CreateTable` command.
     * @return static
     */
    public function createTable($table, array $options)
    {
        list($name, $argument) = $this->db->getQueryBuilder()->createTable($table, $options);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Delete an existing table.
     * @param string $table the name of the table.
     * @return static
     */
    public function deleteTable($table)
    {
        list($name, $argument) = $this->db->getQueryBuilder()->deleteTable($table);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Describe a table.
     * @param string $table the name of the table.
     * @return static
     */
    public function describeTable($table)
    {
        list($name, $argument) = $this->db->getQueryBuilder()->describeTable($table);
        return $this->setCommand($name, $argument);
    }

    /**
     * Return whether a table exists or not.
     * @param string $table The name of the table.
     * @return boolean
     */
    public function tableExists($table)
    {
        try {
            $this->describeTable($table)->execute();
            return true;
        } catch (\Aws\DynamoDb\Exception\ResourceNotFoundException $exc) {
            return false;
        }
    }
    
    /**
     * Put a single item in the table.
     * @param string $table   The name of the table.
     * @param array  $value   The values to input.
     * @param array  $options Additional options to the request argument.
     */
    public function putItem($table, array $value, array $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->putItem($table, $value, $options);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Put a single item in the table.
     * @param string $table   The name of the table.
     * @param mixed  $key     The values to input.
     * @param array  $options Additional options to the request argument.
     */
    public function getItem($table, $key, $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->getItem($table, $key, $options);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * @param string $tableName
     * @param array $values
     */
    public function insert($tableName, $values)
    {
        return $this->putItem($tableName, $values);
    }
    
    /**
     * Increase can be done unlimited time, decrease max 4 times a day
     * @param string $tablename
     * @param int $readThroughput
     * @param int $writeThroughput
     * @return array @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html
     */
    public function updateThroughput($tablename, $readThroughput,
            $writeThroughput)
    {
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
     * 
     * @return type
     */
    public function queryOne()
    {
        $response = $this->execute();
        return $response;
    }

    public function queryAll()
    {
        $response = $this->execute();
        return $response->get('Responses');
    }

}
