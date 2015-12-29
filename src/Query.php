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
     * Executes the query and returns all results as an array.
     * @param Connection $db The database connection used to execute the query.
     * If this parameter is not given, the `dynamodb` application component will be used.
     * @return Command
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
     * Returns all object that matches the query.
     * @param Connection $db The dynamodb connection.
     * @return array
     */
    public function all(Connection $db = null)
    {
        return $this->createCommand($db)->queryAll();
    }

    /**
     * Returns one object that matches the query.
     * @param Connection $db The dynamodb connection.
     * @return array
     */
    public function one(Connection $db = null)
    {
        $this->using = self::USING_GET_ITEM;
        return $this->createCommand($db)->queryOne();
    }

    /**
     * Returns the number of records.
     * @param string $q the COUNT expression. This parameter is ignored by this implementation.
     * @param Connection $db the database connection used to execute the query.
     * If this parameter is not given, the `elasticsearch` application component will be used.
     * @return integer number of records
     */
    public function count($q = '*', Connection $db = null)
    {
        
    }

    public function exists($db = null)
    {
        
    }

}
