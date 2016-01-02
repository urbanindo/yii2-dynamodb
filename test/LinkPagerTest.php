<?php

use UrbanIndo\Yii2\DynamoDb\Pagination;
use UrbanIndo\Yii2\DynamoDb\LinkPager;

/**
 * @author Petra Barus <petra.barus@gmail.com>
 */
class LinkPagerTest extends TestCase
{
    protected function setUp()
    {
        parent::setUp();
        $this->mockWebApplication([
            'components' => [
                'urlManager' => [
                    'scriptUrl' => '/'
                ]
            ]
        ]);
    }
    
    public function testButtons()
    {
        $pagination = new Pagination([
            'route' => 'item/list',
            'lastKey' => 5,
            'nextLastKey' => 10,
            'pageSize' => 10,
        ]);
        
        $linkPagerOutput = LinkPager::widget([
            'pagination' => $pagination,
        ]);
        
        $this->assertContains('<li class="first"><a href="/?r=item%2Flist&amp;per-page=10">1</a></li>', $linkPagerOutput);
        $this->assertContains('<li class="next"><a href="/?r=item%2Flist&amp;last-key=10&amp;per-page=10">&raquo;</a></li>', $linkPagerOutput);
    }
}
