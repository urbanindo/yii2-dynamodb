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
        $faker = \Faker\Factory::create();
        foreach (range(1, 10) as $id) {
            $model = new test\data\Customer([
                'id' => $id,
                'name' => $faker->firstNameMale,
                'age' => ($id % 2) + 1,
            ]);
            $model->save(false);
        }

        $dataProvider = new ActiveDataProvider([
            'query' => test\data\Customer::find()->where(['age' => 2]),
            'pagination' => false,
        ]);

        $this->assertCount(5, $dataProvider->getModels());

        $this->assertFalse($dataProvider->getPagination());
    }

    public function testPagination()
    {
        $faker = \Faker\Factory::create();
        foreach (range(1, 50) as $id) {
            $model = new test\data\Customer([
                'id' => $id,
                'name' => $faker->firstNameMale,
                'age' => $id,
            ]);
            $model->save(false);
        }

        // Pagination using filter non key attribute would return less than
        // desired limit.
        $dataProvider1 = new ActiveDataProvider([
            'query' => test\data\Customer::find(),
            'pagination' => [
                'pageSize' => 5,
            ]
        ]);

        $this->assertCount(5, $dataProvider1->getModels());

        $pagination1 = $dataProvider1->getPagination();
        $this->assertNotNull($pagination1->getNextLastKey());

        // Pagination using filter non key attribute would return less than
        // desired limit.
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
