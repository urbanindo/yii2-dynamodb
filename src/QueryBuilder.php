<?php
/**
 * QueryBuilder class file.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\base\BaseObject;
use yii\helpers\ArrayHelper;
use yii\base\InvalidParamException;
use InvalidArgumentException;
use Exception;

/**
 * QueryBuilder builds an elasticsearch query based on the specification given
 * as a [[Query]] object.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class QueryBuilder extends BaseObject
{
    /**
     * The prefix for automatically generated query binding parameters.
     */
    const PARAM_PREFIX = ':dqp';

    /**
     * The prefix for attribute name.
     */
    const ATTRIBUTE_PREFIX = '#n';

    /**
     * The database connection.
     *
     * @var Connection
     */
    public $db;

    /**
     * Map of query condition to builder methods.
     *
     * These methods are used by [[buildCondition]] to build SQL conditions from array syntax.
     *
     * @var array
     */
    protected $conditionBuilders = [
        'NOT' => 'buildNotCondition',
        'AND' => 'buildAndOrCondition',
        'OR' => 'buildAndOrCondition',
        'BETWEEN' => 'buildBetweenCondition',
        'IN' => 'buildInCondition',
        'ATTRIBUTE_EXISTS' => 'buildFunctionCondition',
        'ATTRIBUTE_NOT_EXISTS' => 'buildFunctionCondition',
        'ATTRIBUTE_TYPE' => 'buildFunctionCondition2Param',
        'BEGINS_WITH' => 'buildFunctionCondition2Param',
        'CONTAINS' => 'buildFunctionCondition2Param',
        // 'SIZE' => 'buildFunctionCondition',
    ];

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
     * @throws InvalidArgumentException Table name should be exist.
     */
    public function build(Query $query)
    {
        $query = $query->prepare($this);

        // Validate query
        if (empty($query->from)) {
            throw new InvalidArgumentException('Table name not set');
        }

        if ($query->using == Query::USING_AUTO) {
            $keyCondition = $this->isConditionMatchKeySchema($query);
            $supportBatchGetItem = $this->isOperatorSupportBatchGetItem($query->where);
            if (empty($query->where) || !empty($query->indexBy) || !empty($query->limit)
                        || !empty($query->offset) || !empty($query->orderBy)
                        || $keyCondition != 1 || !$supportBatchGetItem) {
                // WARNING AWS SDK not support operator beside '=' if use Query method
                // TODO Slice where clause query
                if (!empty($query->orderBy) && ($keyCondition == 1 || $keyCondition == 2)
                        && $supportBatchGetItem) {
                    $query->using = Query::USING_QUERY;
                } else {
                    $query->using = Query::USING_SCAN;
                }
            } else {
                if ($this->isOperatorSupportOnlyGetItem($query->where)) {
                    $query->using = Query::USING_GET_ITEM;
                } else {
                    $query->using = Query::USING_BATCH_GET_ITEM;
                }
            }
        }

        $call = 'build' . $query->using;

        // Call builder
        return $this->{$call}($query);
    }

    /**
     * Find out where all operator condition is able to support BatchGetItem or not.
     * @param array $where Where condition string.
     * @return boolean True when operator support BatchGetItem.
     */
    public function isOperatorSupportBatchGetItem($where)
    {
        if (empty($where)) {
            return false;
        }
        if (is_string($where) || is_numeric($where)) {
            return true;
        }
        // Array type remaining
        if (ArrayHelper::isIndexed($where)) {
            foreach ($where as $whereIndexedElement) {
                if (is_array($whereIndexedElement)) {
                    return !$this->isOperatorSupportBatchGetItem($whereIndexedElement);
                } else {

                    return in_array($whereIndexedElement, ['IN', '=']);
                }
            }
        } else { // associative array remaining
            foreach ($where as $key => $whereElement) {
                if (is_array($whereElement)) {
                    return !$this->isOperatorSupportBatchGetItem($whereElement);
                } // else scalar value
            }
        }

        return true;
    }

    /**
     * Find out where all operator condition is able to support BatchGetItem or not.
     * @param array $where Where condition string.
     * @return boolean True when operator support GetItem.
     */
    public function isOperatorSupportOnlyGetItem($where)
    {
        if (empty($where)) {
            return false;
        }
        if (is_string($where) || is_numeric($where)) {
            return true;
        }
        // Array type remaining
        if (ArrayHelper::isIndexed($where)) {
            foreach ($where as $whereIndexedElement) {
                if (is_array($whereIndexedElement)) {
                    return false;
                } else {
                    return in_array($whereIndexedElement, ['=']);
                }
            }
        } else { // associative array remaining
            foreach ($where as $key => $whereElement) {
                if (is_array($whereElement)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * To check conditoin which contain any key.
     * @param Query $query Object from which the query will be generated.
     * @return integer Return 0 if no match with key, 1 if match only key,
     * 2 if not found all keys but no have non key attribute, 3 if found both
     * key and not key.
     */
    public function isConditionMatchKeySchema(Query $query)
    {
        if (is_array($query->where)) {
            foreach ($this->getKeySchema($query) as $keySchemaElement) {
                $keySchema[] = $keySchemaElement['AttributeName'];
                $allKeyTrue[$keySchemaElement['AttributeName']] = 0;
            }
            if (!is_array(current($query->where))) {
                return $this->searchAttrInArray([$query->where], $keySchema, $allKeyTrue);
            } else {
                return $this->searchAttrInArray($query->where, $keySchema, $allKeyTrue);
            }
        }

        return 0;
    }

    /**
     * Search keys in where schema compare within key schema.
     * @param array $where      Where schema.
     * @param array $keySchema  Key schema.
     * @param array $allKeyTrue Finding all key in KeySchema exist in where clause.
     * @return integer Return 0 if no match with key, 1 if match only key,
     * 2 if contain both key and non key.
     */
    public function searchAttrInArray($where, $keySchema, &$allKeyTrue)
    {
        foreach ($where as $key => $whereElement) {
            if (ArrayHelper::isIndexed($where)) {
                if (is_string($whereElement) || is_numeric($whereElement)) {
                    continue;
                }
                if (ArrayHelper::isIndexed($whereElement)) {
                    if ($this->searchAttrInArray($whereElement, $keySchema, $allKeyTrue) == 3) {
                        return 3;
                    }
                } else { // inner element is associative
                    foreach ($whereElement as $attr => $val) {
                        if (is_array($val)) {
                            $this->searchAttrInArray($val, $keySchema, $allKeyTrue);
                        }
                        if (!in_array($attr, $keySchema)) {
                            if (in_array(1, $allKeyTrue)) {
                                return 3;
                            }
                        } else {
                            $allKeyTrue[$attr] = 1;
                        }
                    }
                }
            } else { // associative
                if ($this->searchAttrInArray($whereElement, $keySchema, $allKeyTrue) == 3) {
                    return 3;
                }
                if (!in_array($key, $keySchema)) {
                    if (in_array(1, $allKeyTrue)) {
                        return 3;
                    }
                } else {
                    $allKeyTrue[$key] = 1;
                }
            }
        }

        return !in_array(0, $allKeyTrue) ? 1 : in_array(1, $allKeyTrue) ? 2 : 0;
    }

    /**
     * Gather key schema
     * @param Query $query Object from which the query will be generated.
     * @return array Key schema
     * @throws InvalidArgumentException IndexName not exist in table description.
     */
    public function getKeySchema(Query $query)
    {
        $tableDescription = $this->db->createCommand()->describeTable($query->from)->execute();
        $options = $this->buildOptions($query, false);

        if (isset($options['IndexName'])) {
            if (isset($tableDescription['Table']['LocalSecondaryIndexes'])) {
                foreach ($tableDescription['Table']['LocalSecondaryIndexes'] as $row) {
                    if ($row['IndexName'] == $options['IndexName']) {
                        return $row['KeySchema'];
                    }
                }
            }
            if (isset($tableDescription['Table']['GlobalSecondaryIndexes'])) {
                foreach ($tableDescription['Table']['GlobalSecondaryIndexes'] as $row) {
                    if ($row['IndexName'] == $options['IndexName']) {
                        return $row['KeySchema'];
                    }
                }
            }
            // Throw because not found in both indexes
            throw new InvalidArgumentException('Index is set but not found in table description.');
        }

        // Use global key schema instead
        return $tableDescription['Table']['KeySchema'];
    }

    /**
     * Generates DynamoDB Query from a [[Query]] object use GetItem method
     * @param Query $query Object from which the query will be generated.
     * @return array The generated DynamoDB command configuration.
     * @throws InvalidArgumentException Parameters should be comply with method.
     */
    public function buildGetItem(Query $query)
    {
        if (empty($query->where)) {
            throw new InvalidArgumentException('WHERE clause must not be empty.');
        }
        if (!empty($query->indexBy) || !empty($query->limit) || !empty($query->offset) || !empty($query->orderBy)) {
            throw new InvalidArgumentException($query->using .
                ' is not support parameter beside where and select clause.');
        }

        return $this->getItem(
            $query->from,
            $this->buildWhereGetItem($query),
            $this->buildOptions($query)
        );
    }

    /**
     * Generates DynamoDB Query from a [[Query]] object use BatchGetItem method
     * @param Query $query Object from which the query will be generated.
     * @return array The generated DynamoDB command configuration.
     * @throws InvalidArgumentException Should comply with BatchGetItem condition.
     */
    public function buildBatchGetItem(Query $query)
    {
        if (empty($query->where)) {
            throw new InvalidArgumentException('WHERE clause must not be empty.');
        }
        if (!empty($query->indexBy) || !empty($query->limit) || !empty($query->offset) || !empty($query->orderBy)) {
            throw new InvalidArgumentException($query->using .
                ' is not support parameter beside where and select clause.');
        }

        return $this->batchGetItem(
            $query->from,
            $this->buildWhereGetItem($query),
            [],
            $this->buildOptions($query)
        );
    }

    /**
     * Generates DynamoDB Query from a [[Query]] object use Scan method.
     * @param Query $query Object from which the query will be generated.
     * @return array The generated DynamoDB command configuration.
     * @throws Exception Scan method do not support sorting.
     */
    public function buildScan(Query $query)
    {
        if (!empty($query->orderBy)) {
            throw new Exception($query->using . ' method cannot use ORDER clause.');
        }

        $options = $this->buildOptions($query);
        if (!empty($query->where)) {
            $options = array_merge(
                $options,
                $this->buildWhereQueryScan($query->where)
            );
        }
        return $this->scan($query->from, $options);
    }

    /**
     * Generates DynamoDB Query from a [[Query]] object use Query method
     * @param Query $query Object from which the query will be generated.
     * @return array The generated DynamoDB command configuration.
     */
    public function buildQuery(Query $query)
    {
        $options = $this->buildOptions($query);
        if (!empty($query->where)) {
            $options = array_merge(
                $options,
                $this->buildWhereQueryScan($query->where)
            );
            // TODO Seperate FilterExpression and KeyConditionExpression
            // For now, change all condition to KeyConditionExpression (assumed
            // all where condition use key attributes)

            $options['KeyConditionExpression'] = $options['FilterExpression'];
            unset($options['FilterExpression']);
        }
        return $this->query($query->from, $options);
    }

    /**
     * Generate projection or selection of attribute for DynamoDB query.
     * @param Query $query Object from which the query will be generated.
     * @return array Array of projection options.
     */
    public function buildProjection(Query $query)
    {
        if (!empty($query->select)) {
            return is_array($query->select) ? [
                    'ProjectionExpression' => implode(', ', $query->select)
                ] : [
                    'ProjectionExpression' => $query->select
                ];
        } else {
            return [];
        }
    }

    /**
     * Generate $key parameter associate with query method.
     * @param Query $query Object from which the parameter will be generated.
     * @return array Array of parameter
     * @throws InvalidParamException Param query has to comply.
     */
    public function buildWhereGetItem(Query $query)
    {
        if (!in_array($query->using, [Query::USING_BATCH_GET_ITEM])) {
            return $query->where;
        }
        if (is_string($query->where) || is_numeric($query->where)) {
            return [$query->where];
        }
        // remain array type
        // supported example: ['a' => 'b'], ['IN', 'a', 'b'], [['IN', 'a', 'b']]
        // and combination of it like [['IN', 'a', 'b'], 'c' => 'd']

        $newWhere = [];
        foreach ($query->where as $key => $value) {
            if (is_string($value) || is_numeric($value)) {
                if (ArrayHelper::isIndexed($query->where)) {
                    if ($query->where[0] != '=' && $query->where[0] != 'IN') {
                        throw new InvalidParamException($query->using .
                            " not support operator '" . $query->where[0] . "'.");
                    }
                    if (sizeof($query->where) == 2) {
                        $newWhere[$query->where[0]] = $query->where[1];
                        break;
                    } elseif (sizeof($query->where) != 3) {
                        throw new InvalidParamException('The WHERE element require 2 or 3 elements.');
                    }

                    if (is_array($query->where[2])) {
                        $newWhere[$query->where[1]] = $query->where[2];
                    } else {
                        $newWhere[$query->where[1]] = [$query->where[2]];
                    }
                    break;
                } else {
                    if (isset($newWhere[$key])) {
                        $newWhere[$key] = array_merge($newWhere[$key], [$value]);
                    } else {
                        $newWhere[$key] = [$value];
                    }
                }
            } else { // else just array type, perhaps
                if (ArrayHelper::isIndexed($value)) {
                    if (isset($newWhere[$key])) {
                        $newWhere[$key] = array_merge($newWhere[$key], $value);
                    } else {
                        $newWhere[$key] = $value;
                    }
                }
            }
        }

        return $newWhere;
    }

    /**
     * Build where condition comply with DynamoDB method Query and Scan.
     * @param string|array $condition Condition or where value.
     * @return array The WHERE clause built from [[Query::$where]].
     * @throws Exception Throw when $condition is non array type.
     */
    public function buildWhereQueryScan($condition)
    {
        if (is_string($condition) || is_numeric($condition)) {
            throw new Exception('Condition just accept array type.');
        }
        $params = [];
        $where = $this->buildCondition($condition, $params);

        return $where === '' ? [] : [
                'FilterExpression' => $where,
                'ExpressionAttributeValues' => $this->paramToExpressionAttributeValues($params),
            ];
    }

    /**
     * Parses the condition specification and generates the corresponding filter expression.
     * @param array $params The binding parameters to be populated.
     * @return string The generated array for expression attribute values.
     * @throws Exception Value type just support basic value, temporary.
     */
    public function paramToExpressionAttributeValues($params)
    {
        // TODO Supporting attribute type detection beside single numeric, single
        // string, and single boolean
        foreach ($params as $i => $value) {
            if (is_int($value)) {
                $params[$i] = ['N' => "$value"];
            } elseif (is_string($value)) {
                $params[$i] = ['S' => $value];
            } elseif (is_bool($value)) {
                $params[$i] = ['BOOL' => $value];
            } elseif (is_array($value)) {
                $subValue = current($value);
                if (is_array($subValue)) {
                    if (ArrayHelper::isIndexed($value)) {
                        $params[$i] = ['L' => $value];
                    } else {
                        $params[$i] = ['M' => $value];
                    }
                    continue;
                } elseif (is_int($subValue)) {
                    $params[$i] = ['NS' => "$value"];
                } elseif (is_string($subValue)) {
                    $params[$i] = ['SS' => $value];
                } else {
                    $params[$i] = ['BS' => $value];
                }
            } else {
                $params[$i] = ['B' => $value];
            }
        }
        return $params;
    }

    /**
     * Parses the condition specification and generates the corresponding filter expression.
     * @param string|array $condition The condition specification. Please refer
     * to [[Query::where()]] on how to specify a condition.
     * @param array        $params    The binding parameters to be populated.
     * @return string the generated filter expression
     */
    public function buildCondition($condition, &$params)
    {
        // TODO Convert key name which conflict with reserved key word
        // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
        if (!is_array($condition)) {
            return (string) $condition;
        } elseif (empty($condition)) {
            return '';
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtoupper($condition[0]);
            if (isset($this->conditionBuilders[$operator])) {
                $method = $this->conditionBuilders[$operator];
            } else {
                $method = 'buildSimpleCondition';
            }
            array_shift($condition);
            return $this->{$method}($operator, $condition, $params);
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            return $this->buildHashCondition($condition, $params);
        }
    }

    /**
     * Creates a condition based on column-value pairs.
     * @param array $condition The condition specification.
     * @param array $params    The binding parameters to be populated.
     * @return string The generated SQL expression
     * @throws Exception No NULL value in DynamoDB.
     */
    public function buildHashCondition($condition, &$params)
    {
        $parts = [];
        foreach ($condition as $attribute => $value) {
            if (is_array($value)) {
                // IN condition
                $parts[] = $this->buildInCondition('IN', [$attribute, $value], $params);
            } else {
                if ($value === null) {
                    throw new Exception(__METHOD__ . ' cannot include NULL value.');
                } else {
                    $phName = self::PARAM_PREFIX . count($params);
                    $parts[] = "$attribute=$phName";
                    $params[$phName] = $value;
                }
            }
        }
        return count($parts) === 1 ? $parts[0] : '(' . implode(') AND (', $parts) . ')';
    }

    /**
     * Connects two or more filter expressions with the `AND` or `OR` operator.
     * @param string $operator The operator to use for connecting the given operands.
     * @param array  $operands The SQL expressions to connect.
     * @param array  $params   The binding parameters to be populated.
     * @return string The generated filter expression.
     */
    public function buildAndOrCondition($operator, $operands, &$params)
    {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand, $params);
            }
            if ($operand !== '') {
                $parts[] = $operand;
            }
        }
        if (!empty($parts)) {
            return '(' . implode(") $operator (", $parts) . ')';
        } else {
            return '';
        }
    }

    /**
     * Inverts an SQL expressions with `NOT` operator.
     * @param string $operator The operator to use for connecting the given operands.
     * @param array  $operands The SQL expressions to connect.
     * @param array  $params   The binding parameters to be populated.
     * @return string The generated SQL expression
     * @throws InvalidParamException If wrong number of operands have been given.
     */
    public function buildNotCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 1) {
            throw new InvalidParamException("Operator '$operator' requires exactly one operand.");
        }
        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand, $params);
        }
        if ($operand === '') {
            return '';
        }
        return "$operator ($operand)";
    }

    /**
     * Creates an SQL expressions with the `BETWEEN` operator.
     * @param string $operator The operator to use (e.g. `BETWEEN` or `NOT BETWEEN`).
     * @param array  $operands The first operand is the column name. The second and
     * third operands describe the interval that column value should be in.
     * @param array  $params   The binding parameters to be populated.
     * @return string The generated SQL expression.
     * @throws InvalidParamException If wrong number of operands have been given.
     */
    public function buildBetweenCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new InvalidParamException("Operator '$operator' requires three operands.");
        }
        list($column, $value1, $value2) = $operands;
        $phName1 = self::PARAM_PREFIX . count($params);
        $params[$phName1] = $value1;
        $phName2 = self::PARAM_PREFIX . count($params);
        $params[$phName2] = $value2;
        return "$column $operator $phName1 AND $phName2";
    }

    /**
     * Creates an filter expressions with the `IN` operator.
     * @param string $operator The operator to use (e.g. `IN` or `NOT IN`).
     * @param array  $operands The first operand is the column name. If it is an array
     * a composite IN condition will be generated.
     * The second operand is an array of values that column value should be among.
     * If it is an empty array the generated expression will be a `false` value if
     * operator is `IN` and empty if operator is `NOT IN`.
     * @param array  $params   The binding parameters to be populated.
     * @return string The generated filter expression.
     * @throws Exception If wrong number of operands have been given or no NULL
     * value in DynamoDB.
     */
    public function buildInCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }
        list($column, $values) = $operands;
        if ($values === [] || $column === []) {
            return $operator === 'IN' ? '0=1' : '';
        }
        $values = (array) $values;
        if (count($column) > 1) {
            return $this->buildCompositeInCondition($operator, $column, $values, $params);
        }
        if (is_array($column)) {
            $column = reset($column);
        }
        foreach ($values as $i => $value) {
            if (is_array($value)) {
                $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                throw new Exception(__METHOD__ . ' cannot include NULL value.');
            } else {
                $phName = self::PARAM_PREFIX . count($params);
                $params[$phName] = $value;
                $values[$i] = $phName;
            }
        }
        if (count($values) > 1) {
            return "$column $operator (" . implode(', ', $values) . ')';
        } else {
            $operator = $operator === 'IN' ? '=' : '<>';
            return $column . $operator . reset($values);
        }
    }

    /**
     * Builds filter expression for IN condition
     *
     * @param string $operator The operator to use. Anything could be used e.g. '`IN`'.
     * @param array  $columns  The columns of composite IN condition.
     * @param array  $values   The values of composite IN condition.
     * @param array  $params   The binding parameters to be populated.
     * @return string Filter expression.
     * @throws Exception No NULL value in DynamoDB.
     */
    protected function buildCompositeInCondition($operator, $columns, $values, &$params)
    {
        $vss = [];
        foreach ($values as $value) {
            $vs = [];
            foreach ($columns as $column) {
                if (isset($value[$column])) {
                    $phName = self::PARAM_PREFIX . count($params);
                    $params[$phName] = $value[$column];
                    $vs[] = $phName;
                } else {
                    throw new Exception(__METHOD__ . ' cannot include NULL value.');
                }
            }
            $vss[] = '(' . implode(', ', $vs) . ')';
        }
        foreach ($columns as $i => $column) {
            if (strpos($column, '(') === false) {
                $columns[$i] = $this->db->quoteColumnName($column);
            }
        }
        return '(' . implode(', ', $columns) . ") $operator (" . implode(', ', $vss) . ')';
    }

    /**
     * Creates an filter expressions for function.
     * @param string $func     The operator to use. Anything could be used e.g. `>`, `<=`, etc.
     * @param array  $operands Contains two column names.
     * @param array  $params   The binding parameters to be populated.
     * @return string The generated filter expression.
     * @throws InvalidParamException If wrong number of operands have been given.
     */
    public function buildFunctionCondition($func, $operands, &$params)
    {
        if (count($operands) !== 1) {
            throw new InvalidParamException("Function '$func' requires exactly one operand.");
        }
        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand, $params);
        } else {
            $phName1 = self::PARAM_PREFIX . count($params);
            $params[$phName1] = $operand;
            $operand = $phName1;
        }
        if ($operand === '') {
            return '';
        }
        $func = strtolower($func);
        return "$func ($operand)";
    }

    /**
     * Creates an filter expressions like `"column" operator value`.
     * @param string $func     The operator to use. Anything could be used e.g. `>`, `<=`, etc.
     * @param array  $operands Contains two column names.
     * @param array  $params   The binding parameters to be populated.
     * @return string The generated SQL expression.
     * @throws InvalidParamException If wrong number of operands have been given.
     */
    public function buildFunctionCondition2Param($func, $operands, &$params)
    {
        if (count($operands) !== 2) {
            throw new InvalidParamException("Function '$func' requires exactly two operands.");
        }
        $phName1 = $operands[0];
        $phName2 = self::PARAM_PREFIX . count($params);
        $params[$phName2] = $operands[1];
        $func = strtolower($func);
        return "$func ($phName1, $phName2)";
    }

    /**
     * Creates an filter expressions like `"column" operator value`.
     * @param string $operator The operator to use. Anything could be used e.g. `>`, `<=`, etc.
     * @param array  $operands Contains two column names.
     * @param array  $params   The binding parameters to be populated.
     * @return string the generated filter expression.
     * @throws InvalidParamException If wrong number of operands have been given.
     * @throws Exception No NULL value in DynamoDB.
     */
    public function buildSimpleCondition($operator, $operands, &$params)
    {
        if (count($operands) !== 2) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }
        list($column, $value) = $operands;
        if ($value === null) {
            throw new Exception(__METHOD__ . ' cannot include NULL value.');
        } else {
            $phName = self::PARAM_PREFIX . count($params);
            $params[$phName] = $value;
            return "$column $operator $phName";
        }
    }

    /**
     * Generate options or addition information for DynamoDB query
     * @param Query   $query Object from which the query will be generated.
     * @param boolean $clear Index by should clear after usage, this param
     * give programmer options to clear or not.
     * @return array Another options which used in the query
     * @throws InvalidArgumentException Table name should be exist and IndexName
     * is string type, can not callable.
     */
    public function buildOptions(Query $query, $clear = true)
    {
        $options = [];

        $this->buildOrderBy($query, $options);
        $this->buildIndexBy($query, $options, $clear);
        $this->buildLimit($query, $options);
        $this->buildExclusiveStartKey($query, $options);
        $this->buildConsistentRead($query, $options);
        $this->buildReturnConsumedCapacity($query, $options);
        $this->buildOptionsExpressionAttributeNames($query, $options);
        $this->buildAdditionalArguments($query, $options);

        return array_merge($options, $this->buildProjection($query));
    }

    /**
     * Build the `IndexName` option.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     * @throws InvalidArgumentException Where order index is unrecognized.
     */
    public function buildOrderBy(Query $query, array &$options = [])
    {
        if (empty($query->orderBy)) {
            return;
        }
        $sort = '';
        if (is_array($query->orderBy)) {
            if (ArrayHelper::isIndexed($query->orderBy)) {
                if (sizeof($query->orderBy) > 1) {
                    $query->indexBy = $query->orderBy[0];
                    $sort = $query->orderBy[1];
                } else {
                    $sort = $query->orderBy[0];
                }
            } else {
                $query->indexBy = key($query->orderBy);
                $sort = current($query->orderBy);
            }
        } else {
            if (in_array(strtoupper($query->orderBy), ['ASC', 'DESC'])) {
                $sort = $query->orderBy;
            } else {
                list($index, $sort) = explode(' ', $query->orderBy, 2);
                $query->indexBy = $index;
            }
        }
        $sort = strtoupper($sort);
        if (!in_array($sort, ['ASC', 'DESC'])) {
            throw new InvalidArgumentException('Sort key unknown: ' . reset($query->orderBy));
        }
        $options['ScanIndexForward'] = ($sort == 'ASC');
    }

    /**
     * Build the `IndexName` option.
     * @param Query   $query   The query to build.
     * @param array   $options The options for command that is being built.
     * @param boolean $clear   Index by should clear after usage, this param
     * give programmer options to clear or not.
     * @return void
     * @throws InvalidArgumentException When the parameter is callable.
     */
    public function buildIndexBy(Query $query, array &$options = [], $clear = true)
    {

        if (empty($query->indexBy)) {
            return;
        }
        if (is_callable($query->indexBy)) {
            throw new InvalidArgumentException('Cannot using callable parameter.');
        }
        $options = array_merge($options, ['IndexName' => $query->indexBy]);
        if ($clear) {
            $query->indexBy = null;
        }
    }

    /**
     * Build the `Limit` option.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     */
    public function buildLimit(Query $query, array &$options = [])
    {
        if (!empty($query->limit)) {
            $options['Limit'] = (int) $query->limit;
        }
    }

    /**
     * Build the `ExclusiveStartKey` option.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     * @throws InvalidArgumentException The keys has to be
     * associative array.
     */
    public function buildExclusiveStartKey(Query $query, array &$options = [])
    {
        if (empty($query->offset)) {
            return;
        }
        if (!is_array($query->offset)) {
            throw new InvalidArgumentException(
                'Missed associative array of keys mapping.'
            );
        }
        $options['ExclusiveStartKey'] = $query->offset;
    }

    /**
     * Build the `ConsistentRead` option.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     * @throws InvalidArgumentException Only accept boolean.
     */
    public function buildConsistentRead(Query $query, array &$options = [])
    {
        if (is_null($query->consistentRead)) {
            return;
        }
        if (!is_bool($query->consistentRead)) {
            throw new InvalidArgumentException(
                'Unsupported consistent read value. Accept boolean type.'
            );
        }
        $options['ConsistentRead'] = $query->consistentRead;
    }

    /**
     * Build the `ReturnConsumedCapacity` option.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     * @throws InvalidArgumentException Unrecognized consume capacity.
     */
    public function buildReturnConsumedCapacity(Query $query, array &$options = [])
    {
        if (empty($query->returnConsumedCapacity)) {
            return;
        }
        $query->returnConsumedCapacity = strtoupper($query->returnConsumedCapacity);
        if (!in_array($query->returnConsumedCapacity, ['INDEXES', 'TOTAL', 'NONE'])) {
            throw new InvalidArgumentException(
                'Unsupported return consumed capacity value:' .
                $query->returnConsumedCapacity
            );
        }
        $options['ReturnConsumedCapacity'] = $query->returnConsumedCapacity;
    }

    /**
     * Build the `ExpressionAttributeNames` option.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     */
    public function buildOptionsExpressionAttributeNames(Query $query, array &$options = [])
    {
        if (!empty($query->expressionAttributeNames)) {
            $options['ExpressionAttributeNames'] = $query->expressionAttributeNames;
        }
    }

    /**
     * Attach the additional arguments.
     * @param Query $query   The query to build.
     * @param array $options The options for command that is being built.
     * @return void
     */
    public function buildAdditionalArguments(Query $query, array &$options = [])
    {
        if (!empty($query->additionalArguments)) {
            $options = array_merge($query->additionalArguments);
        }
    }

    /**
     * Get subtitution for an attribute name.
     * @param Query  $query     The query to build.
     * @param string $attribute The name of the attribute.
     * @return string The subtution name for the attribute.
     */
    public function putExpressionAttributeName(Query $query, $attribute)
    {
        $name = array_search($query->expressionAttributeNames, $attribute);
        if ($name !== false) {
            return $name;
        }
        $count = count($query->expressionAttributeNames);
        $newName = self::ATTRIBUTE_PREFIX . ($count + 1);
        $query->expressionAttributeNames[$newName] = $attribute;

        return $newName;
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
     * Builds a DynamoDB command to update table.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $options Options for the argument.
     * @return array The update table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function updateTable($table, array $options = [])
    {
        $name = 'UpdateTable';
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
        $name = 'PutItem';
        $argument = array_merge([
            'TableName' => $table,
            'Item' => Marshaler::marshalItem($value),
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
        if (count($keySchema) > 1) {
            throw new \InvalidArgumentException('Can not use scalar key argument on table with multiple key');
        }
        $keyName = $keySchema[0]['AttributeName'];
        return [
            $keyName => Marshaler::marshalValue($key),
        ];
    }

    /**
     * @param array $keySchema The schema of the key in the table.
     * @param array $keys      The key as indexed key or associative key.
     * @return array
     */
    public function buildGetItemCompositeKey(array $keySchema, array $keys)
    {
        $keyArgument = [];
        if (ArrayHelper::isIndexed($keys)) {
            foreach ($keys as $i => $value) {
                $keyArgument[$keySchema[$i]['AttributeName']] = Marshaler::marshalValue($value);
            }
        } else {
            foreach ($keys as $i => $value) {
                $keyArgument[$i] = Marshaler::marshalValue($value);
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

        $tableArgument = [
            $table => array_merge([
                'Keys' => $this->buildBatchKeyArgument($table, $keys),
            ], $requestItemOptions)
        ];

        $argument = array_merge(['RequestItems' => $tableArgument], $options);
        return [$name, $argument];
    }

    /**
     * Resolve the keys into `BatchGetItem` eligible argument.
     * @param string $table The name of the table.
     * @param array  $keys  The keys.
     * @return array
     */
    private function buildBatchKeyArgument($table, $keys)
    {
        $tableDescription = $this->db->createCommand()->describeTable($table)->execute();
        $keySchema = $tableDescription['Table']['KeySchema'];

        if (ArrayHelper::isIndexed($keys)) {
            $isScalar = is_string($keys[0]) || is_numeric($keys[0]);
            if ($isScalar) {
                return $this->buildBatchKeyArgumentFromIndexedArrayOfScalar($keySchema, $keys);
            } elseif (ArrayHelper::isIndexed($keys[0])) {
                return $this->buildBatchKeyArgumentFromIndexedArrayOfIndexedArray($keySchema, $keys);
            } else {
                return $this->buildBatchKeyArgumentFromIndexedArrayOfAssociativeArray($keySchema, $keys);
            }
        } else {
            return $this->buildBatchGetItemFromAssociativeArray($keySchema, $keys);
        }
    }

    /**
     * @param array $keySchema The KeySchema of the table.
     * @param array $keys      Indexed array of scalar element.
     * @return array
     * @throws \InvalidArgumentException When the table has multiple key.
     */
    private function buildBatchKeyArgumentFromIndexedArrayOfScalar(array $keySchema, array $keys)
    {
        if (count($keySchema) > 1) {
            throw new \InvalidArgumentException('Can not use scalar key argument on table with multiple key');
        }
        $attribute = $keySchema[0]['AttributeName'];
        return array_map(function ($key) use ($attribute) {
            return [
                $attribute => Marshaler::marshalValue($key),
            ];
        }, $keys);
    }

    /**
     * @param array $keySchema The KeySchema of the table.
     * @param array $keys      Indexed array of indexed array.
     * @return array
     * @throws \InvalidArgumentException When the table has multiple key.
     */
    private function buildBatchKeyArgumentFromIndexedArrayOfIndexedArray(array $keySchema, array $keys)
    {
        return array_map(function ($key) use ($keySchema) {
            $return = [];
            foreach ($key as $i => $value) {
                $return[$keySchema[$i]['AttributeName']] = Marshaler::marshalValue($value);
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
    private function buildBatchKeyArgumentFromIndexedArrayOfAssociativeArray(array $keySchema, array $keys)
    {
        $keySchema;
        return array_map(function ($key) {
            $return = [];
            foreach ($key as $i => $value) {
                $return[$i] = Marshaler::marshalValue($value);
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
        return $this->buildBatchKeyArgumentFromIndexedArrayOfAssociativeArray($keySchema, $indexedKey);
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

    /**
     * Builds a DynamoDB command to put multiple items.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $values  The value to put into the table.
     * @param array  $options The value to put into the table.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function batchPutItem($table, array $values, array $options = [])
    {
        $name = 'BatchWriteItem';
        $requests = array_map(function ($value) {
            return [
                'PutRequest' => [
                    'Item' => Marshaler::marshalItem($value),
                ]
            ];
        }, $values);
        $argument = array_merge([
            'RequestItems' => [
                $table => $requests,
            ]
        ], $options);
        return [$name, $argument];
    }

    /**
     * Builds a DynamoDB command for batch delete item.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $keys    The keys of the row to get.
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
     * @param array  $options Additional options for the final argument.
     * @return array The create table request syntax. The first element is the name of the command,
     * the second is the argument.
     */
    public function batchDeleteItem($table, array $keys, array $options = [])
    {
        $name = 'BatchWriteItem';
        $keyArgument = $this->buildBatchKeyArgument($table, $keys);
        $deleteRequests = array_map(function ($key) {
            return [
                'DeleteRequest' => [
                    'Key' => $key,
                ]
            ];
        }, $keyArgument);

        $argument = array_merge([
            'RequestItems' => [
                $table => $deleteRequests,
            ]
        ], $options);

        return [$name, $argument];
    }

    /**
     * Builds a DynamoDB command for batch delete item.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $keys    The keys of the row to get.
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
     * @param array  $updates Update hash key-value of the model.
     * @param array  $options Additional options for the final argument.
     * @return array
     */
    public function updateItem($table, array $keys, array $updates, array $options = [])
    {
        return updateItemSelectedAction($table, $keys, $updates, 'PUT', $options);
    }

    /**
     * Builds a DynamoDB command for batch delete item.
     *
     * @param string $table   The name of the table to be created.
     * @param array  $keys    The keys of the row to get.
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
     * @param array  $updates Update hash key-value of the model.
     * @param string $action  Action of the method, either 'PUT'|'ADD'|'DELETE'.
     * @param array  $options Additional options for the final argument.
     * @return array
     */
    public function updateItemSelectedAction($table, array $keys, array $updates, $action, array $options = [])
    {
        $name = 'UpdateItem';
        $argument['TableName'] = $table;
        if (ArrayHelper::isIndexed($keys)) {
            $argument['Key'] = $this->buildBatchKeyArgument($table, $keys);
        } else {
            $argument['Key'] = $this->paramToExpressionAttributeValues($keys);
        }
        $value_map = $this->paramToExpressionAttributeValues($updates);
        foreach ($value_map as $key => $value) {
            $argument['AttributeUpdates'][$key] = [
                'Value' => $value,
                'Action' => $action,
            ];
        }
        $argument = array_merge($argument, $options);
        return [$name, $argument];
    }
}
