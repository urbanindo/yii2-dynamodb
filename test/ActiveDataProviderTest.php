<?php

use UrbanIndo\Yii2\DynamoDb\ActiveDataProvider;

class ActiveDataProviderTest extends TestCase
{
    protected function setUp()
    {
        parent::setUp();
        $this->createCustomersTable();
    }
    
    public function testWithoutPagination()
    {
        $command = $this->createCommand();
        
        list($tableName, $fieldName1) = $this->createSimpleTableWithHashKey();
        
        $faker = Faker\Factory::create();
        
        $values = array_map(function ($id) use ($faker, $fieldName1) {
            return [
                $fieldName1 => $faker->uuid,
                'Field2' => $id,
            ];
        }, range(1, 25));
        
        $command->batchPutItem($tableName, $values)->execute();
        
        $dataProvider = new ActiveDataProvider([
            'query' => test\data\Customer::find(),
            'pagination' => false,
        ]);
        
        $this->assertCount(25, $dataProvider->getModels());
        
        $this->assertFalse($dataProvider->getPagination());
    }
    
    public function testPagination()
    {
        $command = $this->createCommand();
        
        list($tableName, $fieldName1) = $this->createSimpleTableWithHashKey();
        
        $faker = Faker\Factory::create();
        
        $values = array_map(function ($id) use ($faker, $fieldName1) {
            return [
                $fieldName1 => $faker->uuid,
                'Field2' => $id,
            ];
        }, range(1, 25));
        
        $command->batchPutItem($tableName, $values)->execute();
        
        $dataProvider1 = new ActiveDataProvider([
            'query' => test\data\Customer::find(),
            'pagination' => [
                'pageSize' => 5,
            ]
        ]);
        
        $this->assertCount(5, $dataProvider1->getModels());
        
        $pagination1 = $dataProvider1->getPagination();
        $this->assertNotNull($pagination1->getNextLastKey());
        
        $dataProvider2 = new ActiveDataProvider([
            'query' => test\data\Customer::find(),
            'pagination' => [
                'lastKey' => $pagination1->getNextLastKey(),
                'pageSize' => 5,
            ]
        ]);
        
        $this->assertCount(5, $dataProvider2->getModels());
    }
}
