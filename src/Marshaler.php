<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace UrbanIndo\Yii2\DynamoDb;

/**
 * Description of Marshaller
 *
 * @author adinata
 */
class Marshaler {
    /* @var $singleton Marshaler */
    private static $singleton = null;
    
    /**
     * 
     * @return Marshaler
     */
    private static function marshaler() {
        if (self::$singleton == null) {
            self::$singleton = new \Aws\DynamoDb\Marshaler();
        }
        return self::$singleton;
    }
    public static function marshal($item) {
        if ($item instanceof \yii\base\Model) {
            $val = self::marshalModel($item);
        } else {
            $val = self::marshalItem($item);
        }
        return $val;
    }
    public static function marshalItem($item) {
        return self::marshaler()->marshalItem($item);
    }
    public static function marshalModel(\yii\base\Model $item) {
        return self::marshaler()->marshalItem($item->getAttributes());
    }
    public static function marshalJson($json) {
        return self::marshaler()->marshalJson($json);
    }
    public static function marshalValue($value) {
        return self::marshaler()->marshalValue($value);
    }
    public static function unmarshalItem(array $data) {
        return self::marshaler()->unmarshalItem($data);
    }
    public static function unmarshalModel(array $data, $class) {
        $object = new $class();
        if (!($object instanceof \yii\base\Model)) {
            throw new \InvalidArgumentException("Class to unmarshal must an instance of \yii\base\Model");
        }
        $object->setAttributes(self::marshaler()->unmarshalItem($data));
        return $object;
    }
    public static function unmarshalJson($json) {
        return self::marshaler()->unmarshalJson($json);
    }
    public static function unmarshalValue($value) {
        return self::marshaler()->unmarshalValue($value);
    }
    
}
