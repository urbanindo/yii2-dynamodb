<?php

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use yii\base\Component;
use yii\db\QueryInterface;
use yii\db\QueryTrait;

/**
 * Description of Query
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Query extends Component implements QueryInterface 
{
    use QueryTrait;
    
    /**
     * Executes the query and returns all results as an array.
     * @param Connection $db the database connection used to execute the query.
     * If this parameter is not given, the `dynamodb` application component will be used.
     * @return array the query results. If the query results in nothing, an empty array will be returned.
     */
    public function all($db = null) {
        
    }
    
    /**
     * Executes the query and returns a single row of result.
     * @param Connection $db the database connection used to execute the query.
     * If this parameter is not given, the `dynamodb` application component will be used.
     * @return array|boolean the first row (in terms of an array) of the query result. False is returned if the query
     * results in nothing.
     */
    public function one($db = null) {
        
    }
    
    public function count($q = '*', $db = null) {
        
    }

    public function exists($db = null) {
        
    }

}
