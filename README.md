# DynamoDB extensions for Yii2

This is a DynamoDB extension for Yii2


[![Latest Stable Version](https://poser.pugx.org/urbanindo/yii2-dynamodb/v/stable.svg)](https://packagist.org/packages/urbanindo/yii2-dynamodb)
[![Total Downloads](https://poser.pugx.org/urbanindo/yii2-dynamodb/downloads.svg)](https://packagist.org/packages/urbanindo/yii2-dynamodb)
[![Latest Unstable Version](https://poser.pugx.org/urbanindo/yii2-dynamodb/v/unstable.svg)](https://packagist.org/packages/urbanindo/yii2-dynamodb)
[![Build Status](https://travis-ci.org/urbanindo/yii2-dynamodb.svg)](https://travis-ci.org/urbanindo/yii2-dynamodb)

## Requirement

This extension requires
- PHP minimum 7.0 and before 7.2 (Upgrade is still underwork)
- Yii2 minimum 2.0.13 and lesser than 2.1
- AWS PHP SDK 3.28

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

Because DynamoDB have different behavior from MySQL, there are several
limitations or behavior change applied. There are several method to get data from
DynamoDB: __GetItem__, __BatchGetItem__, __Scan__, and __Query__.

1. We have tried to implement automatic method to acquire model from Query. You should
assign method explicitly if you want to force the method to use.
2. Not yet support attribute name aliasing (In MySQL known as field aliasing).
3. When using __Query__ method, where condition just support filter by key attributes.
In next roll out we will add filtering with non key attributes.
4. To make pagination, we forcedly using __Query__ method when WHERE condition is set.
Because if you use filtering with non key attribute, it is possible the model result(s)
will less than desired limit value.
5. `indexBy` and `orderBy` cannot use with attribute string value or callable parameter.
This will use as string value and assign to `IndexName` parameter in DynamoDB. To
use sorting, this will forcedly use __QUERY__ method and `orderBy` parameter should be
either `['myIndex' => 'ASC']` or `['myIndex', 'DESC']` and key condition expression
should be defined.
6. Not support NULL and any kind of set attribute type.
7. Not support attribute aliasing belong to Reserve Keywords, which means all attributes
do not using any [Reserve Keywords](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html).
8. When use LinkPager, do not forget use ActiveDataProvider from this package. When
the pagination pass into any kind of Widget View, several components maybe unsupported
like _SerialColumn_, unnecessary total items in summary, and sorting.
