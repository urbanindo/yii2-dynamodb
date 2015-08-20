<?php

/**
 * QueryBuilder class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\base\Object;

/**
 * QueryBuilder builds an elasticsearch query based on the specification given 
 * as a [[Query]] object.
 * @author Petra Barus <petra.barus@gmail.com>
 */
class QueryBuilder extends Object {

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
     * @param \UrbanIndo\Yii2\DynamoDb\Query $query object from which the query will be generated.
     */
    public function build(Query $query) {
        
    }

}
