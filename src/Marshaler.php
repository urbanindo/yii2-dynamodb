<?php
/**
 * Marshaller class file.
 * @author Muhammad Adinata <mail.dieend@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

/**
 * Marshaller wraps AWS DynamoDB Marshaller.
 *
 * @author adinata
 */
class Marshaler
{
    /**
     * @var static
     */
    private static $singleton = null;

    /**
     * Singleton method.
     * @return Marshaler
     */
    private static function marshaler()
    {
        if (self::$singleton == null) {
            self::$singleton = new \Aws\DynamoDb\Marshaler();
        }
        return self::$singleton;
    }

    /**
     * Marshal object either Yii2 model or basic PHP native array.
     * @param mixed $item Item to be marshalled.
     * @return mixed
     */
    public static function marshal($item)
    {
        if ($item instanceof \yii\base\Model) {
            $val = self::marshalModel($item);
        } else {
            $val = self::marshalItem($item);
        }
        return $val;
    }

    /**
     * Marshal a native PHP array of data to a new array that is formatted in
     * the proper parameter structure required by DynamoDB operations.
     *
     * @param array|\stdClass $item An associative array of data.
     *
     * @return array
     */
    public static function marshalItem($item)
    {
        return self::marshaler()->marshalItem($item);
    }

    /**
     * Marshal a Yii2 model object to a new array that is formatted in
     * the proper parameter structure required by DynamoDB operations.
     *
     * @param \yii\base\Model $item
     * @return mixed
     */
    public static function marshalModel(\yii\base\Model $item)
    {
        return self::marshaler()->marshalItem($item->getAttributes());
    }

    /**
     * Marshal a JSON document from a string to an array that is formatted in
     * the proper parameter structure required by DynamoDB operations.
     *
     * @param string $json A valid JSON document.
     * @return array
     * @throws \InvalidArgumentException if the JSON is invalid.
     */
    public static function marshalJson($json)
    {
        return self::marshaler()->marshalJson($json);
    }

    /**
     * Marshal a native PHP value into an array that is formatted in the proper
     * parameter structure required by DynamoDB operations.
     *
     * @param mixed $value A scalar, array, or stdClass value.
     *
     * @return array Formatted like `array(TYPE => VALUE)`.
     * @throws \UnexpectedValueException if the value cannot be marshaled.
     */
    public static function marshalValue($value)
    {
        return self::marshaler()->marshalValue($value);
    }

    /**
     * Unmarshal an item from a DynamoDB operation result into a native PHP
     * array. If you set $mapAsObject to true, then a stdClass value will be
     * returned instead.
     *
     * @param array $data Item from a DynamoDB result.
     *
     * @return array|\stdClass
     */
    public static function unmarshalItem(array $data)
    {
        return self::marshaler()->unmarshalItem($data);
    }

    /**
     * Unmarshal a value from a DynamoDB operation result into a native Yii2 mode.
     * Will return a scalar, array, or (if you set $mapAsObject to true)
     * stdClass value.
     *
     * @param array  $data  Value from a DynamoDB result.
     * @param string $class The name of the class.
     *
     * @return \yii\base\Model
     * @throws \UnexpectedValueException
     */
    public static function unmarshalModel(array $data, $class)
    {
        $object = new $class();
        if (!($object instanceof \yii\base\Model)) {
            throw new \InvalidArgumentException("Class to unmarshal must an instance of \yii\base\Model");
        }
        $object->setAttributes(self::marshaler()->unmarshalItem($data));
        return $object;
    }

    /**
     * Unmarshal a document (item) from a DynamoDB operation result into a JSON
     * document string.
     *
     * @param array $data            Item/document from a DynamoDB result.
     * @param int   $jsonEncodeFlags Flags to use with `json_encode()`.
     *
     * @return string
     */
    public static function unmarshalJson($json)
    {
        return self::marshaler()->unmarshalJson($json);
    }

    /**
     * Unmarshal a value from a DynamoDB operation result into a native PHP
     * value. Will return a scalar, array, or (if you set $mapAsObject to true)
     * stdClass value.
     *
     * @param array $value       Value from a DynamoDB result.
     * @param bool  $mapAsObject Whether maps should be represented as stdClass.
     *
     * @return mixed
     * @throws \UnexpectedValueException
     */
    public static function unmarshalValue($value)
    {
        return self::marshaler()->unmarshalValue($value);
    }
}
