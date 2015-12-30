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

    public static function secondaryIndex() {
        return ['index1'];
    }

    public static function keySecondayIndex() {
        return ['index1' => ['name']];
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
