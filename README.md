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
        // ...
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

## Limitation

Because DynamoDB have different behavior with MySQL in general, there are several
limitations or behavior change applied. There are several method to get data from
DynamoDB: __GetItem__, __BatchGetItem__, __Scan__, and __Query__.

1. We have tried to implement automatic method to acquire model from Query. You have
to assign method explicitly when you want to force method in use.
2. Not yet support attribute name aliasing (In MySQL known as field aliasing).
3. When using __Query__ method, in where condition should using just key attributes.
In next roll out will add filtering with non key attributes.
4. To make pagination, we recommend using Query method when want to filter result.
If you use filtering with non key attribute, it is possible result the model(s) less
than desired limit value.
5. `indexBy` and `orderBy` cannot use by attribute string value or callable parameter.
This will use as string value and assign to `IndexName` parameter in DynamoDB. To
use sorting, it will use __QUERY__ method and `orderBy` parameter should be either
`['myIndex' => 'ASC']` or `['myIndex', 'ASC']`.
6. Not support NULL type attribute.
