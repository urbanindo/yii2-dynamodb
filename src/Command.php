<?php
/**
 * Command class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Aws\DynamoDb\DynamoDbClient;
use Aws\DynamoDb\Marshaler;
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
    public function setCommand($name, array $argument)
    {
        $this->name = $name;
        $this->argument = $argument;
        return $this;
    }

    /**
     * Create new table.
     * @param string $table   The name of the table.
     * @param array  $options Valid options for `CreateTable` command.
     * @return static
     */
    public function createTable($table, array $options)
    {
        list($name, $argument) = $this->db->getQueryBuilder()->createTable($table, $options);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Delete an existing table.
     * @param string $table The name of the table.
     * @return static
     */
    public function deleteTable($table)
    {
        list($name, $argument) = $this->db->getQueryBuilder()->deleteTable($table);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Describe a table.
     * @param string $table The name of the table.
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
     * @return static
     */
    public function putItem($table, array $value, array $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->putItem($table, $value, $options);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Get a single item from table.
     * @param string $table   The name of the table.
     * @param mixed  $key     The key of the row.
     * @param array  $options Additional options to the request argument.
     * @return static
     */
    public function getItem($table, $key, array $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->getItem($table, $key, $options);
        return $this->setCommand($name, $argument);
    }
    
    /**
     * Get multiple items from table using keys.
     *
     * @param string $table   The name of the table.
     * @param array  $keys    The keys of the row. This can be indexed array of
     * scalar value, indexed array of array of scalar value, indexed array of
     * associative array.
     * @param array  $options Additional options to the request argument.
     * @return static
     * @see QueryBuilder::batchGetItem
     */
    public function batchGetItem($table, array $keys, array $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->batchGetItem($table, $keys, $options);
        return $this->setCommand($name, $argument);
    }
}
