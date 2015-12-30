<?php

class ActiveDataProviderTest extends TestCase
{
    
    public function testPagination()
    {
        $command = $this->createCommand();
        
        list($tableName, $fieldName1) = $this->createSimpleTableWithHashKey();
        
        $values = array_map(function ($id) use ($fieldName1, $faker) {
            return [
                $fieldName1 => $faker->uuid,
            ];
        }, range(1, 50));
        
        
    }
}
