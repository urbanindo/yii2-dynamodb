<?php

/**
 * QueryBuilder class file.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\base\Object;
use Aws\DynamoDb\Marshaler;

/**
 * QueryBuilder builds an elasticsearch query based on the specification given
 * as a [[Query]] object.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class QueryBuilder extends Object
{
    /**
     * @var Connection the database connection.
     */
    public $db;

    /**
     * Constructor.
     * @param Connection $connection the database connection.
     * @param array $config name-value pairs that will be used to initialize the object properties
     */
    public function __construct(Connection $connection, $config = [])
    {
        $this->db = $connection;
        parent::__construct($config);
    }

    /**
     * Generates DynamoDB Query from a [[Query]] object.
     * @param Query $query object from which the query will be generated.
     * @return array the generated DynamoDB command configuration
     */
    public function build(Query $query)
    {
        
    }
    
    /**
     * Builds a DynamoDB command to create table.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $options Additional options for the argument.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function createTable($table, array $options  = []) {
        $name = 'CreateTable';
        $argument = array_merge(['TableName' => $table], $options);
        return [$name, $argument];
    }
    
    /**
     * Builds a DynamoDB command to describe table.
     *
     * @param string $table   The name of the table to be created.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function describeTable($table) {
        $name = 'DescribeTable';
        $argument = ['TableName' => $table];
        return [$name, $argument];
    }
    
    /**
     * Builds a DynamoDB command to delete table.
     *
     * @param string $table   The name of the table to be deleted.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function deleteTable($table) {
        $name = 'DeleteTable';
        $argument = ['TableName' => $table];
        return [$name, $argument];
    }

    /**
     * Builds a DynamoDB command to put item.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $value   The value to put into the table.
     * @param array  $options The value to put into the table.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function putItem($table, array $value, array $options = []) {        
        $marshaler = new Marshaler();
        $name = 'PutItem';
        $argument = array_merge([
            'TableName' => $table,
            'Item' => $marshaler->marshalItem($value)
        ], $options);
        return [$name, $argument];
    }
    
    /**
     * Builds a DynamoDB command to get item.
     *
     * @param string $table   The name of the table to be created.
     * @param mixed  $key   The value to put into the table.
     * @param array  $options The value to put into the table.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function getItem($table, $key, array $options = []) {        
        $marshaler = new Marshaler();
        $name = 'GetItem';
        
        //TODO build the argument base don key.
        if (is_string($key) || is_numeric($key)) {
            
        } else {
            
        }
        return [$name, $argument];
    }
}
