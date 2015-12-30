<?php
/**
 * Query class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use yii\base\Component;
use yii\db\QueryInterface;
use yii\db\QueryTrait;
use yii\base\NotSupportedException;
use yii\helpers\ArrayHelper;

/**
 * Query represents item fetching operation from DynamoDB table.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Query extends Component implements QueryInterface
{

    use QueryTrait;

    /**
     * If the query is BatchGetItem operation, meaning the query is for multiple item using keys.
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
     */
    const USING_BATCH_GET_ITEM = 'BatchGetItem';
    
    /**
     * If the query is GetItem operation, meaning the query is for a single item using the key.
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
     */
    const USING_GET_ITEM = 'GetItem';
    
    /**
     * If the query is Query operation, meaning it uses primary key or secondary key from the table.
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
     */
    const USING_QUERY = 'Query';
    
    /**
     * If the query is Scan operation, meaning it will access every item in the table.
     * @link http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
     */
    const USING_SCAN = 'Scan';
    
    /**
     * This will try to detect the auto.
     */
    const USING_AUTO = 'Auto';
    
    /**
     * The name of the key of the row to store the response data.
     */
    const RESPONSE_KEY_PARAM = '_response';

    /**
     * Array of attributes being selected. It will be used to build Projection Expression.
     * @var array
     */
    public $select = [];

    /**
     * Type of query that will be executed, 'Get', 'BatchGet', 'Query', or 'Scan'. Defaults to 'BatchGet'.
     * @var string
     * @see from()
     */
    public $using = self::USING_AUTO;
    
    /**
     * Whether to use consistent read or not.
     * @var boolean
     */
    public $consistentRead;

    /**
     * Whether to return consumed capacity or not.
     * @var boolean
     */
    public $returnConsumedCapacity;

    /**
     * The table to query on.
     * @var string
     */
    public $from;
    
    /**
     * Whether to store response data in the data returned. This can be either boolean
     * false if not to store response data or the key of the response to store.
     * @var array|boolean
     */
    public $storeResponseData = ['ConsumedCapacity', 'LastEvaluatedKey', 'ScannedCount', 'Count'];

    /**
     * Creates a DB command that can be used to execute this query.
     * @param Connection $db The database connection used to generate the SQL statement.
     * If this parameter is not given, the `db` application component will be used.
     * @return Command the created DB command instance.
     */
    public function createCommand(Connection $db = null)
    {
        if ($db === null) {
            $db = Yii::$app->get('dynamodb');
        }
        $config = $db->getQueryBuilder()->build($this);

        return $db->createCommand($config);
    }
    
    
    /**
     * Creates command and execute the query.
     * @param Connection $db The database connection used.
     * @return array The raw response.
     */
    public function execute(Connection $db = null)
    {
        $command = $this->createCommand($db);
        return $command->execute();
    }

    /**
     * Identifies one or more attributes to retrieve from the table.
     * These attributes can include scalars, sets, or elements of a JSON document.
     *
     * @param string|array $attributes The attributes to be selected.
     * Attributes can be specified in either a string separated with comma (e.g. "id, name")
     * or an array (e.g. ['id', 'name']).
     * See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html .
     * @param array        $expression Expression attributes name.
     * See http://amzn.to/1JFZP1n
     * (e.g. [''MyKey' => '#mk'].
     * @return static
     */
    public function select($attributes, array $expression = [])
    {
        if (!is_array($attributes)) {
            $attributes = preg_split(
                '/\s*,\s*/',
                trim($attributes),
                -1,
                PREG_SPLIT_NO_EMPTY
            );
        }
        if (empty($this->select)) {
            $this->select = $attributes;
        } else {
            $this->select = array_merge($this->select, $attributes);
        }
        if (!empty($expression)) {
            foreach ($expression as $exp => $alias) {
                $this->withExpressionAttributesName($exp, $alias);
            }
        }

        return $this;
    }

    /**
     * Sets the table name for the query.
     * @param string|array $table The table(s) to be selected from.
     * @return static the query object itself.
     */
    public function from($table)
    {
        $this->from = $table;
        return $this;
    }

    /**
     * Whether to use consistent read in the query.
     * @return static
     */
    public function withConsistentRead()
    {
        $this->consistentRead = true;
        return $this;
    }

    /**
     * Whether to not use consistent read in the query.
     * @return static
     */
    public function withoutConsistentRead()
    {
        $this->consistentRead = false;
        return $this;
    }

    /**
     * Whether to return the consumed capacity.
     * @return static
     */
    public function withConsumedCapacity()
    {
        $this->returnConsumedCapacity = true;
        return $this;
    }

    /**
     * Whether not to return the consumed capacity.
     * @return static
     */
    public function withoutConsumedCapacity()
    {
        $this->returnConsumedCapacity = false;
        return $this;
    }
    
    /**
     * @param array $response The raw response result from operation.
     * @return array array of values.
     */
    public function getItemsFromResponse($response)
    {
        if (in_array($this->using, [self::USING_QUERY, self::USING_SCAN])) {
            $rows = array_map(function ($item) {
                return Marshaler::unmarshalItem($item);
            }, $response['Items']);
        } else if ($this->using == self::USING_BATCH_GET_ITEM) {
            $rows = array_map(function ($item) {
                return Marshaler::unmarshalItem($item);
            }, $response['Responses'][$this->from]);
        } else if ($this->using == self::USING_GET_ITEM) {
            $row = Marshaler::unmarshalItem($response['Item']);
            $rows = [$row];
        }
        
        $storedResponse = self::extractStoredResponseData($this->storeResponseData, $response);
        if (!empty($storedResponse)) {
            $rows = array_map(function ($row) use ($storedResponse) {
                $row[self::RESPONSE_KEY_PARAM] = $storedResponse;
                return $row;
            }, $rows);
        }
        
        return $rows;
    }
    
    /**
     * @param mixed $responseKeys List of keys to store from operation response, false if don't want to store.
     * @param array $response     The raw response from operation.
     * @return array Stored response data.
     */
    private static function extractStoredResponseData($responseKeys, $response)
    {
        if ($responseKeys == false) {
            return [];
        }
        $return = [];
        foreach ($responseKeys as $key) {
            if (empty($value = ArrayHelper::getValue($response, $key))) {
                continue;
            }
            $return[$key] = $value;
        }
        return $return;
    }
    
    /**
     * Converts the raw query results into the format as specified by this query.
     * This method is internally used to convert the data fetched from database
     * into the format as required by this query.
     * @param array $rows The rows resulted from response parsing.
     * @return array the converted query result
     */
    public function populate($rows)
    {
        if ($this->indexBy === null) {
            return $rows;
        }
        $result = [];
        foreach ($rows as $row) {
            if (is_string($this->indexBy)) {
                $key = $row[$this->indexBy];
            } else if (is_array($this->indexBy)) {
                $key = $row[$this->indexBy[0]] . $row[$this->indexBy[1]];
            } else {
                $key = call_user_func($this->indexBy, $row);
            }
            $result[$key] = $row;
        }
        return $result;
    }

    /**
     * Returns all object that matches the query.
     * @param Connection $db The dynamodb connection.
     * @return array The query results. If the query results in nothing, an empty array will be returned.
     */
    public function all($db = null)
    {
        $response = $this->execute($db);
        $rows = $this->getItemsFromResponse($response);
        return $rows;
    }

    /**
     * Executes the query and returns a single row of result.
     * @param Connection $db The database connection used to generate the SQL statement.
     * If this parameter is not given, the `db` application component will be used.
     * @return array|boolean the first row (in terms of an array) of the query result. False is returned if the query
     * results in nothing.
     */
    public function one($db = null)
    {
        $response = $this->execute($db);
        $rows = $this->getItemsFromResponse($response);
        return $rows;
    }

    /**
     * Returns a value indicating whether the query result contains any row of data.
     * @param Connection $db The database connection used to generate the SQL statement.
     * If this parameter is not given, the `db` application component will be used.
     * @return boolean whether the query result contains any row of data.
     */
    public function exists($db = null)
    {
        $response = $this->execute($db);
        $rows = $this->getItemsFromResponse($response);
        return !empty($rows);
    }
    
    /**
     * Returns the number of records.
     * @param string     $q  The COUNT expression. This parameter is ignored by this implementation.
     * @param Connection $db The database connection used to execute the query.
     * If this parameter is not given, the `elasticsearch` application component will be used.
     * @return void
     * @throws NotSupportedException The count operation is not supported.
     */
    public function count($q = '*', $db = null)
    {
        throw new NotSupportedException('Count operation is not suppported.');
    }
}
