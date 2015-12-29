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
    
    protected static $_primaryKeys = [];
    
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
     * @return ActiveQuery the newly created [[ActiveQuery]] instance.
     */
    public static function find()
    {
        return Yii::createObject(ActiveQuery::className(), [get_called_class()]);
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
     * @param boolean $runValidation whether to perform validation before saving the record.
     * If the validation fails, the record will not be inserted into the database.
     * @param array $attributes list of attributes that need to be saved. Defaults to null,
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

        $ret = $this->getDb()->createCommand()->insert($this->tableName(), $values);

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($values);
        $this->afterSave(true, $changedAttributes);
        
        return $ret;
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
            $client = self::getDb()->getClient();
            $command = $client->getCommand('DescribeTable', [
                'TableName' => self::tableName()
            ]);
            $result = $client->execute($command);
            $keySchema = $result['KeySchema'];
            $keys = [];
            foreach ($keySchema as $key) {
                $idx = $key['KeyType'] == 'HASH' ? 0 : 1;
                $keys[$idx] = $key['AttributeName'];
            }
            self::$_primaryKeys[get_called_class()] = $keys;
        }
        return self::$_primaryKeys[get_called_class()];
    }
    
    public static function batchInsert($values)
    {
        self::getDb()->createCommand()->putItems(static::tableName(), $values);
    }
    
    /**
     * Search for one object.
     * @param array $condition the condition for search.
     * @param array $options addition attribute.
     * @return ActiveRecord
     */
    public static function findOne($condition, $options = null)
    {
        return self::createQueryWithParameter($options)->where($condition)->one();
    }
    
    /**
     * Search for all object that matches condition.
     * @param array $condition the condition for search.
     * @param array $options addition attribute.
     * @return ActiveRecord[]
     */
    public static function findAll($condition, $options = null)
    {
        if ($options == null) {
            $options = ['using' => Query::TYPE_BATCH_GET];
        }
        return self::createQueryWithParameter($options)->where($condition)->all();
    }
    
    /**
     * Create query and assign options if exists.
     * @param array $options
     * @return ActiveQuery the query.
     */
    private static function createQueryWithParameter($options = null)
    {
        $query = self::find();
        if ($options !== null) {
            foreach ($options as $attribute => $value) {
                $query->{$attribute} = $value;
            }
        }
        return $query;
    }

    /**
     * @inheritdoc
     * @todo
     */
    public static function updateAll($attributes, $condition = '')
    {
        parent::updateAll($attributes, $condition);
    }
    
    /**
     * @inheritdoc
     * @todo
     */
    public static function updateAllCounters($counters, $condition = '')
    {
        parent::updateAllCounters($counters, $condition);
    }
    
    /**
     * @inheritdoc
     * @todo
     */
    public static function deleteAll($condition = '', $params = [])
    {
        parent::deleteAll($condition, $params);
    }
}
