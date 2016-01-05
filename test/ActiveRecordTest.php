<?php

class ActiveRecordTest extends TestCase {

    protected function setUp() {
        parent::setUp();
        $this->createCustomersTable();
        $this->createCustomersRangeTable();
    }

    public function testInsertAndFindOne() {

        $this->assertEquals(0, $this->getTableItemCount(\test\data\Customer::tableName()));
        $objectToInsert = new \test\data\Customer();
        $id = (int) \Faker\Provider\Base::randomNumber(5);
        $faker = \Faker\Factory::create();
        $objectToInsert->id = $id;
        $objectToInsert->name = $faker->name;
        $objectToInsert->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert->save(false));
        $this->assertEquals(1, $this->getTableItemCount(\test\data\Customer::tableName()));
        $objectFromFind = \test\data\Customer::findOne(['id' => $id]);

        /* @var $objectFromFind data\Customer */
        $this->assertNotNull($objectFromFind);
        $this->assertEquals($id, $objectFromFind->id);
        $this->assertEquals($objectToInsert->name, $objectFromFind->name);
        $this->assertEquals($objectToInsert->kids, $objectFromFind->kids);
    }

    public function testCondition()
    {
        $objectToInsert = new \test\data\Customer();
        $id1 = (int) \Faker\Provider\Base::randomNumber(5);
        $faker = \Faker\Factory::create();
        $objectToInsert->id = $id1;
        $objectToInsert->name = $faker->name;
        $objectToInsert->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert->save(false));

        $objectToInsert2 = new \test\data\Customer();
        $id2 = $id1 + 1;
        $faker = \Faker\Factory::create();
        $objectToInsert2->id = $id2;
        $objectToInsert2->name = $faker->name;
        $objectToInsert2->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert2->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert2->kids = [
            'Alice',
            'Ari',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert2->save(false));

        $objectToInsert3 = new \test\data\Customer();
        $id3 = $id2 + 1;
        $faker = \Faker\Factory::create();
        $objectToInsert3->id = $id3;
        $objectToInsert3->name = $faker->name;
        $objectToInsert3->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert3->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert3->kids = [
            'Alice',
            'Ari',
            'Angle',
        ];

        $this->assertTrue($objectToInsert3->save(false));

        $objectsFromFind = \test\data\Customer::find()->where(['id' => [$id1]])->all();
        $this->assertEquals(1, count($objectsFromFind));
        $objectsFromFind = \test\data\Customer::find()->where(['id' => $id1])
            ->orWhere(['id' => $id2])->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\Customer::find()->where(['>=', 'id', $id1])->all();
        $this->assertEquals(3, count($objectsFromFind));
        $objectsFromFind = \test\data\Customer::find()->where(['IN', 'id', [$id1, $id2]])->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\Customer::find()->limit(2)->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\Customer::find()->where(['>', 'id', $id1])->all();
        $objectsFromFind = \test\data\Customer::find()->where(['CONTAINS', 'kids', 'Angle'])->all();
        $this->assertEquals(1, count($objectsFromFind));
    }

    public function testCondition2()
    {
        $objectToInsert = new \test\data\CustomerRange();
        $id1 = (int) \Faker\Provider\Base::randomNumber(5);
        $faker = \Faker\Factory::create();
        $objectToInsert->id = $id1;
        $objectToInsert->name = $faker->name;
        $objectToInsert->phone = $faker->phoneNumber;
        $objectToInsert->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert->save(false));

        $objectToInsert2 = new \test\data\CustomerRange();
        $id2 = $id1 + 1;
        $faker = \Faker\Factory::create();
        $objectToInsert2->id = $id2;
        $objectToInsert2->name = $faker->name;
        $objectToInsert2->phone = $faker->phoneNumber;
        $objectToInsert2->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert2->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert2->kids = [
            'Alice',
            'Ari',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert2->save(false));

        $objectToInsert3 = new \test\data\CustomerRange();
        $id3 = $id2 + 1;
        $faker = \Faker\Factory::create();
        $objectToInsert3->id = $id3;
        $objectToInsert3->name = $faker->name;
        $objectToInsert3->phone = $faker->phoneNumber;
        $objectToInsert3->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert3->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert3->kids = [
            'Alice',
            'Ari',
            'Angle',
        ];

        $this->assertTrue($objectToInsert3->save(false));

        $objectsFromFind = \test\data\CustomerRange::find()->where(['id' => [$id1]])->all();
        $this->assertEquals(1, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['id' => $id1])->all();
        $this->assertEquals(1, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['id' => $objectToInsert3->id])
            ->andWhere(['phone' => $objectToInsert3->phone])
            ->indexBy(\test\data\CustomerRange::secondaryIndex()[0])->all();
        $this->assertEquals(1, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['id' => [$id1, $id2]])->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['IN', 'id', [$id1, $id2]])->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['id' => $id1])
            ->orWhere(['id' => $id2])->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['>=', 'id', $id1])->all();
        $this->assertEquals(3, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->limit(2)->all();
        $this->assertEquals(2, count($objectsFromFind));
        $objectsFromFind = \test\data\CustomerRange::find()->where(['>', 'id', $id1])->all();
        $objectsFromFind = \test\data\CustomerRange::find()->where(['CONTAINS', 'kids', 'Angle'])->all();
        $this->assertEquals(1, count($objectsFromFind));
    }

    public function testInsertAndFindAll() {

        $id1 = (int) \Faker\Provider\Base::randomNumber(5);
        $faker = \Faker\Factory::create();
        $objectToInsert1 = new \test\data\Customer();
        $objectToInsert1->id = $id1;
        $objectToInsert1->name = $faker->name;
        $objectToInsert1->contacts = [
            'telephone1' => 123456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert1->prices = [
            1000000,
            1000000,
            1000000,
            1000000
        ];
        $objectToInsert1->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert1->save(false));
        $this->assertEquals(1, $this->getTableItemCount(\test\data\Customer::tableName()));

        $objectsFromFind = \test\data\Customer::findAll(['id' => [$id1]]);

        /* @var $objectFromFind data\Customer */
        $this->assertEquals(1, count($objectsFromFind));

        $id2 = (int) \Faker\Provider\Base::randomNumber(5);
        $objectToInsert2 = new \test\data\Customer();
        $objectToInsert2->id = $id2;
        $objectToInsert2->name = $faker->name;
        $objectToInsert2->contacts = [
            'telephone2' => 223456,
            'telephone2' => 345678,
            'telephone3' => 345678,
        ];
        $objectToInsert2->prices = [
            2000000,
            2000000,
            2000000,
            2000000
        ];
        $objectToInsert2->kids = [
            'Alice',
            'Billy',
            'Charlie',
        ];

        $this->assertTrue($objectToInsert2->save(false));

        $objectsFromFind2 = \test\data\Customer::findAll(['id' => [$id1, $id2]]);

         /* @var $objectFromFind data\Customer */
        $this->assertEquals(2, count($objectsFromFind2));

        $this->assertTrue($objectsFromFind2[0]->id = $id1 || $objectsFromFind2[0]->id = $id2);
    }
}
