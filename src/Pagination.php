<?php

namespace UrbanIndo\Yii2\DynamoDb;

class Pagination extends \yii\data\Pagination
{
    /**
     * This will only return 1.
     * @return integer number of pages
     */
    public function getPageCount()
    {
        return 1;
    }
}
