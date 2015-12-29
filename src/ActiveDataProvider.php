<?php
/**
 * ActiveDataProvider class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use yii\base\InvalidParamException;
use yii\di\Instance;
use yii\helpers\ArrayHelper;

/**
 * ActiveDataProvider implements a data provider based on DynamoDB Query and ActiveQuery.
 *
 * ActiveDataProvider provides data by performing DB queries using [[query]].
 *
 * The following is an example of using ActiveDataProvider to provide ActiveRecord instances:
 *
 * ```php
 * $provider = new ActiveDataProvider([
 *     'query' => Post::find(),
 *     'pagination' => [
 *         'pageSize' => 20,
 *     ],
 * ]);
 *
 * // get the posts in the current page
 * $posts = $provider->getModels();
 * ```
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class ActiveDataProvider extends \yii\data\BaseDataProvider
{
    /**
     * @var Query the query that is used to fetch data models and [[totalCount]]
     * if it is not explicitly set.
     */
    public $query;
    
    /**
     * @var Connection|array|string the DB connection object or the application component ID of the DB connection.
     * If not set, the default DB connection will be used.
     */
    public $db = 'dynamodb';
    
    /**
     * Initializes the DB connection component.
     * This method will initialize the [[db]] property to make sure it refers to a valid DB connection.
     * @return void
     * @throws InvalidConfigException If [[db]] is invalid.
     */
    public function init()
    {
        parent::init();
        if (is_string($this->db)) {
            $this->db = Instance::ensure($this->db, Connection::className());
        }
    }

    /**
     * Prepares the keys associated with the currently available data models.
     * @param array $models The available data models.
     * @return array the keys.
     */
    protected function prepareKeys(array $models)
    {
        $models;
        return [];
    }

    /**
     * Prepares the data models that will be made available in the current page.
     * @return array the available data models
     * @throws \yii\base\InvalidConfigException If the query is not class of UrbanIndo\Yii2\DynamoDb\Query
     */
    protected function prepareModels()
    {
        if (!$this->query instanceof Query) {
            throw new InvalidConfigException('The "query" property must be an instance of a class that implements the UrbanIndo\Yii2\DynamoDb\Query or its subclasses.');
        }
        $query = clone $this->query;
        if (($pagination = $this->getPagination()) !== false) {
            $pagination->totalCount = $this->getTotalCount();
            $query->limit($pagination->getLimit());
            $query->offset($pagination->getOffset());
        }
        
        $models = $query->all($this->db);
        if ($pagination !== false) {
            $peek = current(array_slice($models, -1));
            /* @var $peek ActiveRecord */
            $nextLastKey = ArrayHelper::getValue($peek->getResponseData(), 'LastEvaluatedKey');
            $pagination->setNextLastKey($nextLastKey);
        }
        return $models;
    }

    /**
     * Returns a value indicating the total number of data models in this data provider.
     * @return integer total number of data models in this data provider.
     */
    protected function prepareTotalCount()
    {
        if (!$this->query instanceof Query) {
            throw new InvalidConfigException('The "query" property must be an instance of a class that implements the UrbanIndo\Yii2\DynamoDb\Query or its subclasses.');
        }
        $query = clone $this->query;
        return (int) $query->limit(-1)->orderBy([])->count('*', $this->db);
    }
    
    /**
     * Returns the pagination object used by this data provider.
     * Note that you should call [[prepare()]] or [[getModels()]] first to get correct values
     * of [[Pagination::totalCount]] and [[Pagination::pageCount]].
     * @return Pagination|boolean the pagination object. If this is false, it means the pagination is disabled.
     */
    public function getPagination()
    {
        if ($this->_pagination === null) {
            $this->setPagination([]);
        }

        return $this->_pagination;
    }
    
    /**
     * Sets the pagination for this data provider.
     * @param array|Pagination|boolean $value the pagination to be used by this data provider.
     * This can be one of the following:
     *
     * - a configuration array for creating the pagination object. The "class" element defaults
     *   to 'UrbanIndo\Yii2\DynamoDb\Pagination'
     * - an instance of [[Pagination]] or its subclass
     * - false, if pagination needs to be disabled.
     *
     * @throws InvalidParamException
     */
    public function setPagination($value)
    {
        if (is_array($value)) {
            $config = ['class' => Pagination::className()];
            if ($this->id !== null) {
                $config['pageSizeParam'] = $this->id . '-per-page';
            }
            parent::setPagination(Yii::createObject(array_merge($config, $value)));
        } elseif ($value instanceof Pagination || $value === false) {
            parent::setPagination($value);
        } else {
            throw new InvalidParamException('Only Pagination instance, configuration array or false is allowed.');
        }
    }
}
