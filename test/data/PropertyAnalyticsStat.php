<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace test\data;

/**
 * Description of PropertyAnalyticsStats
 *
 * @author adinata
 */
class PropertyAnalyticsStat extends \UrbanIndo\Yii2\DynamoDb\ActiveRecord {
    public static function tableName() {
        return 'PropertyAnalyticsStats';
    }
    
    public static function primaryKey() {
        return ['propertyId', 'day'];
    }
    
    public function attributes() {
        return [
            'value',
        ];
    }
}
