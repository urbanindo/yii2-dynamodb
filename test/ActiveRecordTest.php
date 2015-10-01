<?php

class ActiveRecordTest extends TestCase {
    
    public function testInsertAndFindOne() {
        
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
        $objectFromFind = \test\data\Customer::findOne(['name' => $faker->name], ['using' => UrbanIndo\Yii2\DynamoDb\Query::TYPE_SCAN]);
       
        /* @var $objectFromFind data\Customer */
        $this->assertNotNull($objectFromFind);
        $this->assertEquals($id, $objectFromFind->id);
        $this->assertEquals($objectToInsert->name, $objectFromFind->name);
    }
}
