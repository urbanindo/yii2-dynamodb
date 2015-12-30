# DynamoDB extensions for Yii2

This is a DynamoDB extension for Yii2


[![Latest Stable Version](https://poser.pugx.org/urbanindo/yii2-dynamodb/v/stable.svg)](https://packagist.org/packages/urbanindo/yii2-queue)
[![Total Downloads](https://poser.pugx.org/urbanindo/yii2-dynamodb/downloads.svg)](https://packagist.org/packages/urbanindo/yii2-queue)
[![Latest Unstable Version](https://poser.pugx.org/urbanindo/yii2-dynamodb/v/unstable.svg)](https://packagist.org/packages/urbanindo/yii2-queue)
[![Build Status](https://travis-ci.org/urbanindo/yii2-dynamodb.svg)](https://travis-ci.org/urbanindo/yii2-queue)

## Requirement

This extension requires
- PHP 5.4
- Yii2
- AWS PHP SDK 2.8

## Installation

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
php composer.phar require --prefer-dist urbanindo/yii2-dynamodb "*"
```

or add

```
"urbanindo/yii2-dynamodb": "*"
```

to the require section of your `composer.json` file.

## Setting Up

After the installation, sets the `dynamodb` component in the config.

```php
return [
    // ...
    'components' => [
        //
        'dynamodb' => [
            'class' => 'UrbanIndo\Yii2\DynamoDb\Connection',
            'config' => [
                //This is the config used for Aws\DynamoDb\DynamoDbClient::factory()
                //See http://docs.aws.amazon.com/aws-sdk-php/v2/guide/service-dynamodb.html#factory-method
                'credentials' => [
                    'key'    => 'YOUR_AWS_ACCESS_KEY_ID',
                    'secret' => 'YOUR_AWS_SECRET_ACCESS_KEY',
                ],
                'region' => 'ap-southeast-1',
            ]
        ]
    ],
];
```

