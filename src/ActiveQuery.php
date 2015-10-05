<?php

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\db\ActiveQueryInterface;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRelationTrait;

/**
 * ActiveQuery represents a [[Query]] associated with an [[ActiveRecord]] class.
 *
 * An ActiveQuery can be a normal query or be used in a relational context.
 * 
 * ActiveQuery instances are usually created by [[ActiveRecord::find()]].
 * Relational queries are created by [[ActiveRecord::hasOne()]] and [[ActiveRecord::hasMany()]].
 *
 * Normal Query
 * ------------
 *
 * ActiveQuery mainly provides the following methods to retrieve the query results:
 *
 * - [[one()]]: returns a single record populated with the first row of data.
 * - [[all()]]: returns all records based on the query results.
 * - [[count()]]: returns the number of records.
 * - [[scalar()]]: returns the value of the first column in the first row of the query result.
 * - [[column()]]: returns the value of the first column in the query result.
 * - [[exists()]]: returns a value indicating whether the query result has data or not.
 * 
 * @author Petra Barus <petra.barus@gmail.com>
 */
class ActiveQuery extends Query implements ActiveQueryInterface {
    
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * @event Event an event that is triggered when the query is initialized via [[init()]].
     */
    const EVENT_INIT = 'init';

    /**
     * Constructor.
     * @param array $modelClass the model class associated with this query
     * @param array $config configurations to be applied to the newly created query object
     */
    public function __construct($modelClass, $config = []) {
        $this->modelClass = $modelClass;
        parent::__construct($config);
    }

    /**
     * Initializes the object.
     * This method is called at the end of the constructor. The default implementation will trigger
     * an [[EVENT_INIT]] event. If you override this method, make sure you call the parent implementation at the end
     * to ensure triggering of the event.
     */
    public function init() {
        parent::init();
        $this->trigger(self::EVENT_INIT);
    }
    
    /**
     * @param Connection $db
     * @return ActiveRecord
     */
    public function one($db = null) {
        /* @var $response \Guzzle\Service\Resource\Model */
        $response = parent::one($db);
        $value = $response->get('Item');
        $marshaller = new \Aws\DynamoDb\Marshaler();
        return $this->createModel($value, $marshaller); 
    }
    
    /**
     * @param Connection $db
     * @return ActiveRecord[]
     */
    public function all($db = null) {
        $responses = parent::all($db);
        $modelClass = $this->modelClass;
        $marshaller = new \Aws\DynamoDb\Marshaler();
        return array_map(function($value) use ($marshaller) {
            return $this->createModel($value, $marshaller);
        }, $responses[$modelClass::tableName()]);
    }
    
    /**
     * Create model base on return.
     * @param type $value
     * @param Aws\DynamoDb\Marshaler $marshaller
     * @return \UrbanIndo\Yii2\DynamoDb\modelClass
     */
    private function createModel($value, \Aws\DynamoDb\Marshaler $marshaller = null) {
        $model = new $this->modelClass;
        if (!isset($marshaller)) {
            $marshaller = new \Aws\DynamoDb\Marshaler();
        }
        $model->setAttributes($marshaller->unmarshalItem($value), false);
        return $model;
    }

//    public function asArray($value = true) {}

//    public function batch($batchSize = 100, $db = null) {}
}
