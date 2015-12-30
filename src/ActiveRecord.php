<?php
/**
 * ActiveRecord class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use yii\db\BaseActiveRecord;
use yii\helpers\Inflector;
use yii\helpers\StringHelper;
use yii\helpers\ArrayHelper;

/**
 * ActiveRecord is the base class for classes representing relational data in terms of objects.
 *
 * Active Record implements the [Active Record design pattern](http://en.wikipedia.org/wiki/Active_record) for
 * [DynamoDB] (https://aws.amazon.com/dynamodb/).
 *
 * For defining a record a subclass should at least implement the [[attributes()]] method to define
 * attributes and the [[tableName()]] methods to define the table name that the class represents.
 *
 * The following is an example model called `Customer`:
 *
 * ```php
 * class Customer extends \UrbanIndo\Yii2\DynamoDb\ActiveRecord
 * {
 *     public function attributes()
 *     {
 *         return ['id', 'name', 'address', 'registration_date'];
 *     }
 *
 *     public static function tableName() {
 *         return 'Customers';
 *     }
 * }
 * ```
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class ActiveRecord extends BaseActiveRecord
{
    /**
     * Stores the primary keys loaded from table schema.
     * @var array
     */
    protected static $_primaryKeys = [];

    /**
     * Stores the response metadata either from retrieval operation such as
     * <ul>
     *  <li>`ConsumedCapacity` from GetItem, BatchGetItem, Query, and Scan</li>
     *  <li>`UnprocessedKeys` from BatchGetItem</li>
     *  <li>`Count`, `LastEvaluatedKey`, `ScannedCount` from Query or Scan</li>
     * </ul>
     * @var array
     */
    protected $_responseData = [];

    /**
     * Stores the operation type that retrieves this model. Eligible values are.
     * <ul>
     *  <li>Query::USING_BATCH_GET_ITEM</li>
     *  <li>Query::USING_GET_ITEM</li>
     *  <li>Query::USING_QUERY</li>
     *  <li>Query::USING_SCAN</li>
     * </ul>
     * @var string
     */
    protected $_findType;

    /**
     * Returns the database connection used by this AR class.
     * By default, the "dynamodb" application component is used as the database connection.
     * You may override this method if you want to use a different database connection.
     * @return Connection the database connection used by this AR class.
     */
    public static function getDb()
    {
        return Yii::$app->get('dynamodb');
    }

    /**
     * Declares the name of the database table associated with this AR class.
     * By default this method returns the class name as the table name by calling [[Inflector::camel2id()]].
     * @return string the table name
     */
    public static function tableName()
    {
        return Inflector::camel2id(StringHelper::basename(get_called_class()), '_');
    }

    /**
     * @inheritdoc
     * @param array $options Additional options for the query class.
     * @return ActiveQuery the newly created [[ActiveQuery]] instance.
     */
    public static function find($options = [])
    {
        return Yii::createObject(ActiveQuery::className(), array_merge([
            get_called_class(),
        ], $options));
    }

    /**
     * Inserts a document into the associated index using the attribute values of this record.
     *
     * This method performs the following steps in order:
     *
     * 1. call [[beforeValidate()]] when `$runValidation` is true. If validation
     * fails, it will skip the rest of the steps;e
     * 2. call [[afterValidate()]] when `$runValidation` is true.
     * 3. call [[beforeSave()]]. If the method returns false, it will skip the
     * rest of the steps;
     * 4. insert the record into database. If this fails, it will skip the rest of the steps;
     * 5. call [[afterSave()]];
     *
     * In the above step 1, 2, 3 and 5, events [[EVENT_BEFORE_VALIDATE]],
     * [[EVENT_BEFORE_INSERT]], [[EVENT_AFTER_INSERT]] and [[EVENT_AFTER_VALIDATE]]
     * will be raised by the corresponding methods.
     *
     * Only the [[dirtyAttributes|changed attribute values]] will be inserted into database.
     *
     * For example, to insert a customer record:
     *
     * ~~~
     * $customer = new Customer;
     * $customer->name = $name;
     * $customer->email = $email;
     * $customer->insert();
     * ~~~
     *
     * @param boolean $runValidation Whether to perform validation before saving the record.
     * If the validation fails, the record will not be inserted into the database.
     * @param array   $attributes    List of attributes that need to be saved. Defaults to null,
     * meaning all attributes will be saved.
     *
     * @return boolean whether the attributes are valid and the record is inserted successfully.
     */
    public function insert($runValidation = true, $attributes = null)
    {
        if ($runValidation && !$this->validate($attributes)) {
            Yii::info('Model not inserted due to validation error.', __METHOD__);
            return false;
        }
        if (!$this->beforeSave(true)) {
            return false;
        }
        $values = $this->getDirtyAttributes($attributes);

        $ret = $this->getDb()->createCommand()->putItem($this->tableName(), $values)->execute();

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);

        return true;
    }

    /**
     * Returns the primary key **name(s)** for this AR class.
     *
     * Note that an array should be returned even when the record only has a single primary key.
     *
     * For the primary key **value** see [[getPrimaryKey()]] instead.
     *
     * The array returned will consist of either one or two string. The first one
     * will be the name of the HASH key. The second one will be the name of the RANGE
     * key if exists.
     *
     * @return string[] the primary key name(s) for this AR class.
     */
    public static function primaryKey()
    {
        if (!isset(self::$_primaryKeys[get_called_class()])) {
            $description = self::getDb()->createCommand()
                    ->describeTable(self::tableName())
                    ->execute();
            $keySchema = $description['KeySchema'];
            $keys = [];
            foreach ($keySchema as $key) {
                $idx = $key['KeyType'] == 'HASH' ? 0 : 1;
                $keys[$idx] = $key['AttributeName'];
            }
            self::$_primaryKeys[get_called_class()] = $keys;
        }
        return self::$_primaryKeys[get_called_class()];
    }

    /**
     * Batch insert values into the table.
     * @param array $values The values to be inserted.
     * @return mixed
     */
    public static function batchInsert($values)
    {
        return self::getDb()->createCommand()->putItems(static::tableName(), $values);
    }

    /**
     * Search for one object.
     * @param mixed $condition The condition for search.
     * @param array $options   Additional attribute.
     * @return static
     */
    public static function findOne($condition, $options = null)
    {
        return self::find($options)->where($condition)->one();
    }

    /**
     * Search for all object that matches condition.
     * @param mixed $condition The condition for search.
     * @param array $options   Additional attribute for the query class.
     * @return static[]
     */
    public static function findAll($condition, $options = null)
    {
        return self::find($options)->where($condition)->all();
    }

    /**
     * Updates the whole table using the provided attribute values and conditions.
     * For example, to change the status to be 1 for all customers whose status is 2:
     *
     * ```php
     * Customer::updateAll(['status' => 1], 'status = 2');
     * ```
     *
     * @param array        $attributes Attribute values (name-value pairs) to be saved into the table.
     * @param string|array $condition  The conditions of the rows.
     * @return void
     * @throws \yii\base\NotSupportedException Not implemented yet.
     */
    public static function updateAll($attributes, $condition = '')
    {
        throw new \yii\base\NotSupportedException(__METHOD__ . ' is not supported.');
    }

    /**
     * Updates the whole table using the provided counter changes and conditions.
     * For example, to increment all customers' age by 1,
     *
     * ```php
     * Customer::updateAllCounters(['age' => 1]);
     * ```
     *
     * @param array        $counters  The counters to be updated (attribute name => increment value).
     * Use negative values if you want to decrement the counters.
     * @param string|array $condition The conditions to select the rows to be updated.
     * @return void
     * @throws \yii\base\NotSupportedException Not implemented yet.
     */
    public static function updateAllCounters($counters, $condition = '')
    {
        throw new \yii\base\NotSupportedException(__METHOD__ . ' is not supported.');
    }

    /**
     * Deletes rows in the table using the provided conditions.
     * WARNING: If you do not specify any condition, this method will delete ALL rows in the table.
     *
     * For example, to delete all customers whose status is 3:
     *
     * ```php
     * Customer::deleteAll('status = 3');
     * ```
     *
     * @param string|array $condition The conditions that will select the rows.
     * @param array        $params    The parameters (name => value) to be bound to the query.
     * @return void
     * @throws \yii\base\NotSupportedException Not implemented yet.
     */
    public static function deleteAll($condition = '', $params = [])
    {
        throw new \yii\base\NotSupportedException(__METHOD__ . ' is not supported.');
    }

    /**
     * Populates an active record object using a row of data from the database/storage.
     *
     * This is an internal method meant to be called to create active record objects after
     * fetching data from the database. It is mainly used by [[ActiveQuery]] to populate
     * the query results into active records.
     *
     * When calling this method manually you should call [[afterFind()]] on the created
     * record to trigger the [[EVENT_AFTER_FIND|afterFind Event]].
     *
     * @param static $record The record to be populated. In most cases this will be an instance
     * created by [[instantiate()]] beforehand.
     * @param array  $row    Attribute values (name => value).
     * @return void
     */
    public static function populateRecord($record, $row)
    {
        $responseData = ArrayHelper::getValue($row, Query::RESPONSE_KEY_PARAM);
        unset($row[Query::RESPONSE_KEY_PARAM]);
        parent::populateRecord($record, $row);

        if (!empty($responseData)) {
            $record->_responseData = $responseData;
        }
    }

    /**
     * Returns the response meta data from BatchGetItem, GetItem, Scan, or Query
     * operation. This can contains `ConsumedCapacity`, `UnprocessedKeys`, `Count`,
     * `LastEvaluatedKey`, `ScannedCount` depends on whether the query enables
     * storing the meta data.
     * @return array
     */
    public function getResponseData()
    {
        return $this->_responseData;
    }

    /**
     * Sets the method how the active record was retrieved. Valid values are
     * <ul>
     *  <li>Query::USING_BATCH_GET_ITEM</li>
     *  <li>Query::USING_GET_ITEM</li>
     *  <li>Query::USING_QUERY</li>
     *  <li>Query::USING_SCAN</li>
     * </ul>
     * @param string $type The type.
     * @return void
     */
    public function setFindType($type)
    {
        $this->_findType = $type;
    }

    /**
     * Returns the method how the active record was retrieved. Valid values are
     * <ul>
     *  <li>Query::USING_BATCH_GET_ITEM</li>
     *  <li>Query::USING_GET_ITEM</li>
     *  <li>Query::USING_QUERY</li>
     *  <li>Query::USING_SCAN</li>
     * </ul>.
     * @return string
     */
    public function getFindType()
    {
        return $this->_findType;
    }
}
