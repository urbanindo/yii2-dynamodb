<?php
/**
 * Command class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use Aws\DynamoDb\DynamoDbClient;

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Command extends \yii\base\BaseObject
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
     * Update table command.
     * @param string $table   The name of the table.
     * @param array  $options The options of the update.
     * @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html
     * @return static
     */
    public function updateTable($table, array $options)
    {
        list($name, $argument) = $this->db->getQueryBuilder()->updateTable($table, $options);
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
     * @param string $table The name of the table.
     * @return integer
     */
    public function getTableItemCount($table)
    {
        $description = $this->describeTable($table)->execute();
        return $description['Table']['ItemCount'];
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
        } catch (\Aws\DynamoDb\Exception\DynamoDbException $exc) {
            if (strpos($exc->getMessage(), 'ResourceNotFoundException') === false) {
                throw $exc;
            }
        }
        return false;
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
     * Put multiple items in the table. This method can only put 25 object max.
     * @param string $table   The name of the table.
     * @param array  $values  The values to input.
     * @param array  $options Additional options to the request argument.
     * @return static
     */
    public function batchPutItem($table, array $values, array $options = [])
    {
        assert(count($values <= 25));
        list($name, $argument) = $this->db->getQueryBuilder()->batchPutItem($table, $values, $options);
        return $this->setCommand($name, $argument);
    }

    /**
     * Put multiple items in the table with size > 25 object.
     * @param string $table   The name of the table.
     * @param array  $values  The values to input.
     * @param array  $options Additional options to the request argument.
     * @return static[]
     */
    public function batchPutAllItems($table, array $values, array $options = [])
    {
        $batches = array_chunk($values, 25);
        return array_map(
            function ($batch) use ($table, $options) {
                $command = clone $this;
                $command->batchPutItem($table, $batch, $options);
                return $command;
            },
            $batches
        );
    }

    /**
     * Execute multiple command.
     * @param array $commands Commands to be executed.
     * @return array The array result of the command execution.
     */
    public static function batchExecute(array $commands)
    {
        /* @var $commands static[] */
        return array_map(
            function ($command) {
                return $command->execute();
            },
            $commands
        );
    }

    /**
     * Put multiple items in the table.
     * @param string $table   The name of the table.
     * @param array  $keys    The keys of the row. This can be indexed array of
     * scalar value, indexed array of array of scalar value, indexed array of
     * associative array.
     * @param array  $options Additional options to the request argument.
     * @return static
     */
    public function batchDeleteItem($table, array $keys, array $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->batchDeleteItem($table, $keys, $options);
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
     * Scan table.
     * @param string $table   The name of the table.
     * @param array  $options Options to the request argument.
     * @return static
     */
    public function scan($table, array $options = [])
    {
        list($name, $argument) = $this->db->getQueryBuilder()->scan($table, $options);
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

    /**
     * Update throughput of a table.
     *
     * This is a shorthand for `updateTable` command for only updating the
     * throughput.
     *
     * Note, the readThroughput and writeThrougput have to be at least 1, and
     * cannot be null.
     *
     * @param string  $table           The name of the table.
     * @param integer $readThroughput  The read throughput new size.
     * @param integer $writeThroughput The write throguhput new size.
     * @return static
     * @throws \InvalidArgumentException When both throughput is empty.
     */
    public function updateThroughput($table, $readThroughput, $writeThroughput)
    {
        return $this->updateTable($table, [
            'ProvisionedThroughput' => [
                'ReadCapacityUnits' => $readThroughput,
                'WriteCapacityUnits' => $writeThroughput,
            ]
        ]);
    }

    /**
     * @param string $table   The name of the Table.
     * @param array  $keys    The keys of the row.
     * @param array  $updates The hash attribute => value will be updated.
     * @param array  $options Additional options to the request argument.
     * @return static
     */
    public function updateItem($table, array $keys, array $updates, array $options = [])
    {
        return $this->updateItemSelectedAction($table, $keys, $updates, 'PUT', $options);
    }

    /**
     * @param string $table   The name of the Table.
     * @param array  $keys    The keys of the row.
     * @param array  $updates The hash attribute => value will be updated.
     * @param string $action  Action of the method, either 'PUT'|'ADD'|'DELETE'.
     * @param array  $options Additional options to the request argument.
     * @return static
     */
    public function updateItemSelectedAction($table, array $keys, array $updates, $action, array $options = [])
    {
        list($name, $query_argument) = $this->db->getQueryBuilder()
            ->updateItemSelectedAction($table, $keys, $updates, $action, $options);
        return $this->setCommand($name, $query_argument);
    }
}
