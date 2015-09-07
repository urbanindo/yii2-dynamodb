<?php

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use yii\base\Component;
use yii\db\QueryInterface;
use yii\db\QueryTrait;
use yii\base\NotSupportedException;

/**
 * Description of Query
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Query extends Component implements QueryInterface
{
    use QueryTrait;
    const TYPE_BATCH_GET = 'BatchGetItem';
    const TYPE_GET = 'GetItem';
    const TYPE_QUERY = 'Query';
    const TYPE_SCAN = 'Scan';


    /**
     * Array of attributes being selected. It will be used to build Projection Expression.
     * @var array
     */
    public $select = [];

    /**
     * @var string Type of query that will be executed, 'Get', 'BatchGet', 'Query', or 'Scan'. Defaults to 'BatchGet'
     * @see from()
     */
    public $using = 'BatchGet';

    /**
     * @var array list of query parameter values indexed by parameter placeholders.
     * For example, `[':name' => 'Dan', ':age' => 31]`.
     */
    public $expressionAttributesNames = [];
    public $expressionAttributesValues = [];
    public $consistentRead;
    public $returnConsumedCapacity;
    public $from;
    public $keys = [];
    /**
     * Executes the query and returns all results as an array.
     * @param Connection $db the database connection used to execute the query.
     * If this parameter is not given, the `dynamodb` application component will be used.
     * @return Command
     */
    public function createCommand($db = null)
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
     * @param string|array $attributes the attributes to be selected. Attributes can be specified in either a string separated with comma (e.g. "id, name") or an array (e.g. ['id', 'name']). see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.AccessingItemAttributes.html
     * @param array $expression expression attributes name. see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ExpressionPlaceholders.html#ExpressionAttributeNames (e.g. [''MyKey' => '#mk']
     * @return Query
     */
    public function select($attributes, $expression = [])
    {
        if (!is_array($attributes)) {
            $attributes = preg_split('/\s*,\s*/', trim($attributes), -1, PREG_SPLIT_NO_EMPTY);
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

    public function from($tableName) {
        $this->from = $tableName;
    }

    public function using($queryType)
    {
        if (!in_array($queryType, [self::TYPE_BATCH_GET, self::TYPE_GET])) {
            throw new NotSupportedException('only batch get and get that is currently supported');
        }
        $this->using = $queryType;
        return $this;
    }

    public function withExpressionAttributesName($attributes, $alias)
    {
        $this->expressionAttributesNames[$attributes] = $alias;
        return $this;
    }

    public function withConsistentRead() {
        $this->consistentRead = true;
        return $this;
    }

    public function withoutConsistentRead() {
        $this->consistentRead = false;
        return $this;
    }
    public function withConsumedCapacity() {
        $this->returnConsumedCapacity = true;
        return $this;
    }
    public function withoutConsumedCapacity() {
        $this->returnConsumedCapacity = false;
        return $this;
    }

    public function all($db = null) {
        return $this->createCommand($db)->queryAll();
    }

    public function count($q = '*', $db = null) {
        // TODO: only if query and scan operations.
        // batch get assumes results equal to number of id and hash
    }
    
    public function exists($db = null) {
        return !empty($this->createCommand($db)->queryOne());
    }

    public function one($db = null) {
        return $this->createCommand($db)->queryOne();
    }

    /**
     * Starts a batch query. Doesn't necessarily have $batchSize size. 
     * Will call one requests for each batch instead of calling until empty like all.
     *
     */
    public function batch($batchSize = 100, $db = null)
    {
        // todo
    }
    
    // todo override where so it can filter primary key or not
}
