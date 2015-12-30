<?php
/**
 * QueryBuilder class file.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\base\Object;
use Aws\DynamoDb\Marshaler;
use yii\helpers\ArrayHelper;

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
     * @param Connection $connection The database connection.
     * @param array      $config     Name-value pairs that will be used to initialize the object properties.
     */
    public function __construct(Connection $connection, array $config = [])
    {
        $this->db = $connection;
        parent::__construct($config);
    }

    /**
     * Generates DynamoDB Query from a [[Query]] object.
     * @param Query $query Object from which the query will be generated.
     * @return array The generated DynamoDB command configuration.
     */
    public function build(Query $query)
    {
        $query;
        return [];
    }
    
    /**
     * Builds a DynamoDB command to create table.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $options Additional options for the argument.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function createTable($table, array $options = [])
    {
        $name = 'CreateTable';
        $argument = array_merge(['TableName' => $table], $options);
        return [$name, $argument];
    }
    
    /**
     * Builds a DynamoDB command to describe table.
     *
     * @param string $table The name of the table to be created.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function describeTable($table)
    {
        $name = 'DescribeTable';
        $argument = ['TableName' => $table];
        return [$name, $argument];
    }
    
    /**
     * Builds a DynamoDB command to delete table.
     *
     * @param string $table The name of the table to be deleted.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function deleteTable($table)
    {
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
    public function putItem($table, array $value, array $options = [])
    {
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
     * @param mixed  $key     The key of the item to get. This can be a scalar
     * (numeric or string) or an indexed array or an associative array.
     * If the key is indexed array, the first element will be the primary key,
     * and the second element will be the secondary key.
     * @param array  $options The additional options for the request.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function getItem($table, $key, array $options = [])
    {
        $name = 'GetItem';
        
        $tableDescription = $this->db->createCommand()->describeTable($table)->execute();
        $keySchema = $tableDescription['Table']['KeySchema'];
        
        if (is_string($key) || is_numeric($key)) {
            $keyArgument = $this->buildGetItemScalarKey($keySchema, $key);
        } else {
            $keyArgument = $this->buildGetItemCompositeKey($keySchema, $key);
        }

        $argument = array_merge(
            ['TableName' => $table],
            ['Key' => $keyArgument],
            $options
        );
        return [$name, $argument];
    }
    
    /**
     * @param array $keySchema The schema of the key in the table.
     * @param mixed $key       The key either string or integer.
     * @return array
     * @throws \InvalidArgumentException When the key in argument is scalar but
     * the table has multiple keys.
     */
    public function buildGetItemScalarKey(array $keySchema, $key)
    {
        $marshaler = new Marshaler();
        if (count($keySchema) > 1) {
            throw new \InvalidArgumentException('Can not use scalar key argument on table with multiple key');
        }
        $keyName = $keySchema[0]['AttributeName'];
        return [
            $keyName => $marshaler->marshalValue($key),
        ];
    }
    
    /**
     * @param array $keySchema The schema of the key in the table.
     * @param array $keys      The key as indexed key or associative key.
     * @return array
     */
    public function buildGetItemCompositeKey(array $keySchema, array $keys)
    {
        $marshaler = new Marshaler();
        
        $keyArgument = [];
        if (ArrayHelper::isIndexed($keys)) {
            foreach ($keys as $i => $value) {
                $keyArgument[$keySchema[$i]['AttributeName']] = $marshaler->marshalValue($value);
            }
        } else {
            foreach ($keys as $i => $value) {
                $keyArgument[$i] = $marshaler->marshalValue($value);
            }
        }
        return $keyArgument;
    }
    
    /**
     * Builds a DynamoDB command for batch get item.
     *
     * @param string $table              The name of the table to be created.
     * @param array  $keys               The keys of the row to get.
     * This can be
     * 1) indexed array of scalar value for table with single key,
     *
     * e.g. ['value1', 'value2', 'value3', 'value4']
     *
     * 2) indexed array of array of scalar value for table with multiple key,
     *
     * e.g. [
     *  ['value11', 'value12'],
     *  ['value21', 'value22'],
     *  ['value31', 'value32'],
     *  ['value41', 'value42'],
     * ]
     *
     * The first scalar will be the primary (or hash) key, the second will be the
     * secondary (or range) key.
     *
     * 3) indexed array of associative array
     *
     * e.g. [
     *  ['attribute1' => 'value11', 'attribute2' => 'value12'],
     *  ['attribute1' => 'value21', 'attribute2' => 'value22'],
     *  ['attribute1' => 'value31', 'attribute2' => 'value32'],
     *  ['attribute1' => 'value41', 'attribute2' => 'value42'],
     * ]
     *
     * 4) or associative of scalar values.
     *
     * e.g. [
     *  'attribute1' => ['value11', 'value21', 'value31', 'value41']
     *  'attribute2' => ['value12', 'value22', 'value32', 'value42']
     * ].
     *
     * @param array  $options            Additional options for the final argument.
     * @param array  $requestItemOptions Additional options for the request item.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function batchGetItem($table, array $keys, array $options = [], array $requestItemOptions = [])
    {
        $name = 'BatchGetItem';
        
        $tableDescription = $this->db->createCommand()->describeTable($table)->execute();
        $keySchema = $tableDescription['Table']['KeySchema'];
        
        if (ArrayHelper::isIndexed($keys)) {
            $isScalar = is_string($keys[0]) || is_numeric($keys[0]);
            if ($isScalar) {
                $keyArgument = $this->buildBatchGetItemFromIndexedArrayOfScalar($keySchema, $keys);
            } elseif (ArrayHelper::isIndexed($keys[0])) {
                $keyArgument = $this->buildBatchGetItemFromIndexedArrayOfIndexedArray($keySchema, $keys);
            } else {
                $keyArgument = $this->buildBatchGetItemFromIndexedArrayOfAssociativeArray($keySchema, $keys);
            }
        } else {
            $keyArgument = $this->buildBatchGetItemFromAssociativeArray($keySchema, $keys);
        }
        
        $tableArgument = array_merge([
            $table => [
                    'Keys' => $keyArgument
            ]
        ], $requestItemOptions);
        
        $argument = array_merge(['RequestItems' => $tableArgument], $options);
        return [$name, $argument];
    }
    
    /**
     * @param array $keySchema The KeySchema of the table.
     * @param array $keys      Indexed array of scalar element.
     * @return array
     * @throws \InvalidArgumentException When the table has multiple key.
     */
    public function buildBatchGetItemFromIndexedArrayOfScalar(array $keySchema, array $keys)
    {
        $marshaler = new Marshaler();
        if (count($keySchema) > 1) {
            throw new \InvalidArgumentException('Can not use scalar key argument on table with multiple key');
        }
        $attribute = $keySchema[0]['AttributeName'];
        return array_map(function ($key) use ($attribute, $marshaler) {
            return [
                $attribute => $marshaler->marshalValue($key),
            ];
        }, $keys);
    }
    
    /**
     * @param array $keySchema The KeySchema of the table.
     * @param array $keys      Indexed array of indexed array.
     * @return array
     * @throws \InvalidArgumentException When the table has multiple key.
     */
    public function buildBatchGetItemFromIndexedArrayOfIndexedArray(array $keySchema, array $keys)
    {
        $marshaler = new Marshaler();
        return array_map(function ($key) use ($keySchema, $marshaler) {
            $return = [];
            foreach ($key as $i => $value) {
                $return[$keySchema[$i]['AttributeName']] = $marshaler->marshalValue($value);
            }
            return $return;
        }, $keys);
    }
    
    /**
     * @param array $keySchema The KeySchema of the table.
     * @param array $keys      Indexed array of associative array.
     * @return array
     * @throws \InvalidArgumentException When the table has multiple key.
     */
    public function buildBatchGetItemFromIndexedArrayOfAssociativeArray(array $keySchema, array $keys)
    {
        $marshaler = new Marshaler();
        $keySchema;
        return array_map(function ($key) use ($marshaler) {
            $return = [];
            foreach ($key as $i => $value) {
                $return[$i] = $marshaler->marshalValue($value);
            }
            return $return;
        }, $keys);
    }
    
    /**
     * @param array $keySchema The KeySchema of the table.
     * @param array $keys      Associative array of indexed scalar.
     * @return array
     * @throws \InvalidArgumentException When the table has multiple key.
     */
    public function buildBatchGetItemFromAssociativeArray(array $keySchema, array $keys)
    {
        $attributes = array_keys($keys);
        $countKeyInEachAttributes = array_values(array_map(function ($key) {
            return count($key);
        }, $keys));
        if (count(array_unique($countKeyInEachAttributes)) != 1) {
            throw new \InvalidArgumentException('The number of keys is not the same');
        }
        $countKey = $countKeyInEachAttributes[0];
        $indexedKey = [];
        foreach (range(1, $countKey) as $i) {
            $k = $i - 1;
            foreach ($attributes as $attribute) {
                $indexedKey[$k][$attribute] = $keys[$attribute][$k];
            }
        }
        return $this->buildBatchGetItemFromIndexedArrayOfAssociativeArray($keySchema, $indexedKey);
    }
    
    /**
     * Builds a DynamoDB command to scan table.
     *
     * @param string $table   The name of the table to scan.
     * @param array  $options The scan options.
     * @return array The scan table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function scan($table, array $options = [])
    {
        $name = 'Scan';
        $argument = array_merge([
            'TableName' => $table,
        ], $options);
        return [$name, $argument];
    }
    
    /**
     * Builds a DynamoDB command to query table.
     *
     * @param string $table   The name of the table to query.
     * @param array  $options The scan options.
     * @return array The query table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function query($table, array $options = [])
    {
        $name = 'Query';
        $argument = array_merge([
            'TableName' => $table,
        ], $options);
        return [$name, $argument];
    }
}
