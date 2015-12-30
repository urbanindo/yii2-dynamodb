<?php

class ActiveDataProviderTest extends TestCase
{
    
    public function testPagination()
    {
        $command = $this->createCommand();
        
        list($tableName, $fieldName1) = $this->createSimpleTableWithHashKey();
        
        $faker = Faker\Factory::create();
    }
}
