<?php

/**
 * QueryBuilder class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\base\InvalidParamException;
use yii\base\NotSupportedException;
use yii\base\Object;

/**
 * QueryBuilder builds an elasticsearch query based on the specification given 
 * as a [[Query]] object.
 * @author Petra Barus <petra.barus@gmail.com>
 */
class QueryBuilder extends Object {
    const PARAM_PREFIX = ':var';
    /**
     * @var Connection the database connection.
     */
    public $db;

    /**
     * Constructor.
     * @param Connection $connection the database connection.
     * @param array $config name-value pairs that will be used to initialize the object properties
     */
    public function __construct(Connection $connection, $config = []) {
        $this->db = $connection;
        parent::__construct($config);
    }
    
    /**
     * Generates DynamoDB Query from a [[Query]] object.
     * @param Query $query object from which the query will be generated.
     * @return array the generated DynamoDB command configuration
     */
    public function build(Query $query) {
        if ($query->using == Query::TYPE_BATCH_GET) {
            $request = $this->buildBatchGetItem($query);
        } else if ($query->using == Query::TYPE_GET) {
            $request = $this->buildGetItem($query);
        }
        return [
            'method'=> $query->using,
            'request' => $request
        ];        
    }

    private function buildGetItem(Query $query) {
        $request = [];
        // required
        $request['Key'] = $this->buildKey($query);
        $request['TableName'] = $this->buildTableName($query);

        // optional
        $this->setCommonRequest($request, $query);
        $this->buildConsumedCapacity($request, $query);
        return $request;
    }
    private function buildConsumedCapacity(&$request, &$query) {
        if (isset($query->returnConsumedCapacity)) {
            $request['ReturnConsumedCapacity'] = $query->returnConsumedCapacity;
        }
    }

    private function buildKey(Query& $query) {
        $condition = $query->where;
        if (!is_array($condition)) {
            throw new NotSupportedException('String conditions in where() are not supported by dynamodb.');
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            throw new InvalidParamException('`Query::TYPE_GET` only supports hash format in condition (e.g. "key"=>"value")');
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            $parts = Marshaler::marshal($condition);
            
            return $parts;
        }
    }
    private function buildBatchGetItem(Query $query) {
        $condition = $query->where;
        
        if (!is_array($condition)) {
            throw new NotSupportedException('String conditions in where() are not supported by dynamodb.');
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            throw new InvalidParamException('`Query::TYPE_GET` only supports hash format in condition (e.g. "key"=>"value")');
        }
        // check size 
        $size = null;
        foreach ($condition as $key => $value) {
            if ($size == null) {
                $size = count($value);
            } else if ($size != count($value)) {
                throw new NotSupportedException('Number of comparison between hash and range query must be equals');
            }
        }
        $tablename = $this->buildTableName($query);

        $request = [
            'RequestItems' => [
                $tablename => [
                    'Keys' => []
                ]
            ]
        ];
        $items = &$request['RequestItems'][$tablename];
        
        foreach (range(0, $size-1) as $i) {
            $item = [];
            foreach ($condition as $key => $value) {
                $item[$key] = $value[$i];
            }
            $items['Keys'][] = Marshaler::marshal($item);
        }

        $this->setCommonRequest($items, $query);
        $this->buildConsumedCapacity($request, $query);
        return $request;
    }
    
    private function buildTableName(&$query) {
        return isset($query->from) ? 
                $query->from : // get table name from active record incase of from not called
                call_user_func([$query->modelClass, 'tableName']);
    }
    private function setCommonRequest(&$item, &$query) {
        if (isset($query->consistentRead)) {
            $item['ConsistentRead'] = $query->consistentRead;
        }
        if (!empty($query->expressionAttributesNames)) {
            $item['ExpressionAttributeNames'] = [];
            foreach ($query->expressionAttributesNames as $key => $expression) {
                $item['ExpressionAttributeNames'][$key] = $expression;
            }
        }
        if (!empty($query->select)) {
            $item['ProjectionExpression'] = $this->buildSelect($query->select);
        }
    }
    
    private function buildSelect($select) {
        return implode(',', $select);
    }
    /**
     * where -> 
     *  key condition expression + expression attribute value, 
     *      - if query type is batch get or get:
     *      - required. check whether hash and range key both are used
     *          - only `=` operator can be used
     * 
     *  filter expression + expression attribute value, 
     *      - in scan type, all uses this
     *      - in query type, only that's not included in query filter here
     * 
     *  query filter + expression attribute value,
     *      - only hash and range key attribute can be filtered, other attributes goes to filter expression
     *      - in range key, only function with following operator allowed
     *          - =, <, <=, >, >=, BETWEEN, begins_with(). Other goes to filter expression
     * select -> 
     *  projection expression
     * 
     * 
     * @staticvar array $builders
     * @param type $condition
     * @return type
     * @throws NotSupportedException
     * @throws InvalidParamException
     */
    private function buildCondition($condition, Query &$query)
    {
        static $builders = [
            'not' => 'buildNotCondition',
            'and' => 'buildAndCondition',
            'or' => 'buildAndCondition',
            'between' => 'buildBetweenCondition',
            'not between' => 'buildBetweenCondition',
            'in' => 'buildInCondition',
            'not in' => 'buildInCondition',
            'like' => 'buildLikeCondition',
            'not like' => 'buildLikeCondition',
            'or like' => 'buildLikeCondition',
            'or not like' => 'buildLikeCondition',
        ];
        if (empty($condition)) {
            return [];
        }
        if (!is_array($condition)) {
            throw new NotSupportedException('String conditions in where() are not supported by dynamodb.');
        }
        
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtolower($condition[0]);
            if (isset($builders[$operator])) {
                $method = $builders[$operator];
                
            } else {
                $method = 'buildSimpleCondition';
            }
            array_shift($condition);
            return $this->$method($operator, $condition, $query);
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            return $this->buildHashCondition($condition, $query);
        }
    }
    private function buildNotCondition($operator, $operands, &$query) {
        if (count($operands) != 1) {
            throw new InvalidParamException("Operator '$operator' requires exactly one operand.");
        }
        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand);
        }
        return [$operator => $operand];
    }

    private function buildAndCondition($operator, $operands, &$query) {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand);
            }
            if (!empty($operand)) {
                $parts[] = $operand;
            }
        }
        if (!empty($parts)) {
            return [$operator => $parts];
        } else {
            return [];
        }
    }
    private function buildBetweenCondition($operator, $operands, &$query) {
        throw new NotSupportedException('not ready');
    }
    private function buildInCondition($operator, $operands, &$query) {
        throw new NotSupportedException('not ready');
    }
    private function buildLikeCondition($operator, $operands) {
        throw new NotSupportedException('like conditions are not supported by dynamodb.');
    }

    /**
     * Creates an SQL expressions like `"column" operator value`.
     * @param string $operator the operator to use. Anything could be used e.g. `>`, `<=`, etc.
     * @param array $operands contains two column names.
     * @param array $params the binding parameters to be populated
     * @return string the generated SQL expression
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildSimpleCondition($operator, $operands, Query &$query)
    {
        if (count($operands) !== 2) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }

        list($column, $value) = $operands;

        $varname = self::PARAM_PREFIX . count($query->expressionAttributesValues);
        $query->expressionAttributesValues[$varname] = Marshaler::marshal($value);
        return "$column $operator $varname";
    }
    /**
     * Creates a condition based on column-value pairs.
     * @param array $condition the condition specification.
     * @param array $params the binding parameters to be populated
     * @return string the generated SQL expression
     */
    public function buildHashCondition($condition, Query &$query)
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value) || $value instanceof Query) {
                // IN condition
                $parts[] = $this->buildInCondition('IN', [$column, $value], $query);
            } else {
                $varname = self::PARAM_PREFIX . count($query->expressionAttributesValues);
                $query->expressionAttributesValues[$varname] = Marshaler::marshal($value);
                $parts[] = "$column=$varname";
            }
        }
        return count($parts) === 1 ? $parts[0] : '(' . implode(') AND (', $parts) . ')';
    }
}
