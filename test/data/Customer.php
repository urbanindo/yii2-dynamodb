<?php

namespace test\data;

/**
 * @property integer $id
 * @property integer $name
 */
class Customer extends \UrbanIndo\Yii2\DynamoDb\ActiveRecord {
    
    public static function tableName() {
        return 'Customers';
    }
    
    public static function primaryKey() {
        return ['id'];
    }
    
    public function attributes() {
        return [
            'id',
            'name',
            'contacts',
            'prices',
            'kids',
        ];
    }
}
