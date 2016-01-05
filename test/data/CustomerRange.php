<?php

namespace test\data;

/**
 * @property integer $id
 * @property integer $name
 */
class CustomerRange extends \UrbanIndo\Yii2\DynamoDb\ActiveRecord {

    public static function tableName() {
        return 'CustomerRanges';
    }

    public static function primaryKey() {
        return ['id', 'name'];
    }

    public static function secondaryIndex() {
        return ['index1'];
    }

    public static function keySecondayIndex() {
        return ['index1' => ['id', 'phone']];
    }

    public function attributes() {
        return [
            'id',
            'name',
            'phone',
            'contacts',
            'prices',
            'kids',
            'age',
        ];
    }
}
