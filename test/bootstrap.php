<?php

define('DYNAMODB_URL', 'http://localhost:2700');

error_reporting(-1);
define('YII_ENABLE_ERROR_HANDLER', false);
define('YII_DEBUG', true);
$_SERVER['SCRIPT_NAME'] = '/' . __DIR__;
$_SERVER['SCRIPT_FILENAME'] = __FILE__;

require dirname(__FILE__) . '/../vendor/autoload.php';
require dirname(__FILE__) . '/../vendor/yiisoft/yii2/Yii.php';
require dirname(__FILE__) . '/TestCase.php';

Yii::setAlias('@test', __DIR__);

$application = new \yii\console\Application([
    'id' => 'Yii2 DynamoDB Test',
    'basePath' => dirname(__FILE__),
    'components' => [
        'dynamodb' => [
            /* @var $dynamodb \UrbanIndo\Yii2\DynamoDb\Connection */
            'class' => '\UrbanIndo\Yii2\DynamoDb\Connection',
            'config' => [
                'credentials' => [
                    'key' => 'AKIA',
                    'secret' => '1234567890',
                ],
                'region' => 'ap-southeast-1',
                'version' => 'latest',
                'endpoint' => DYNAMODB_URL,
            ]
        ]
    ]
]);
