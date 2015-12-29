<?php

use UrbanIndo\Yii2\DynamoDb\Pagination;

class PaginationTest extends TestCase
{
    
    protected function setUp()
    {
        parent::setUp();
        $this->mockWebApplication([
            'components' => [
                'urlManager' => [
                    'scriptUrl' => '/index.php'
                ],
            ],
        ]);
    }
    
    /**
     * Data provider for [[testCreateUrl()]]
     * @return array test data
     */
    public function dataProviderCreateUrl()
    {
        return [
            [
                null,
                null,
                '/index.php?r=item%2Flist',
            ],
            [
                2,
                null,
                '/index.php?r=item%2Flist&last-key=2',
            ],
            [
                2,
                5,
                '/index.php?r=item%2Flist&last-key=2&per-page=5',
            ],
        ];
    }
    
    /**
     * @dataProvider dataProviderCreateUrl
     * 
     * @param string $lastKey
     * @param integer $pageSize
     * @param string $expectedUrl
     */
    public function testCreateUrl($lastKey, $pageSize, $expectedUrl)
    {
        $pagination = new Pagination();
        $pagination->route = 'item/list';
        $this->assertEquals($expectedUrl, $pagination->createUrl($lastKey, $pageSize));
    }
    
    /**
     * Data provider for [[testCreateUrl()]]
     * @return array test data
     */
    public function dataProviderGetLinks()
    {
        return [
            [
                null,
                5,
                null,
                [
                    'self' => '/index.php?r=item%2Flist',
                    'next' => '/index.php?r=item%2Flist&last-key=5',
                ]
            ],
            [
                5,
                10,
                null,
                [
                    'self' => '/index.php?r=item%2Flist&last-key=5',
                    'next' => '/index.php?r=item%2Flist&last-key=10',
                ]
            ],
            [
                5,
                10,
                10,
                [
                    'self' => '/index.php?r=item%2Flist&last-key=5&per-page=10',
                    'next' => '/index.php?r=item%2Flist&last-key=10&per-page=10',
                ]
            ],
        ];
    }
    
    /**
     * @dataProvider dataProviderGetLinks
     * 
     * @param string  $currentLastKey The current last key.
     * @param string  $nextLastKey    The next last key.
     * @param integer $pageSize       The page size to show.
     * @param array   $links          The links resulted
     */
    public function testGetLinks($currentLastKey, $nextLastKey, $pageSize, $links)
    {
        $pagination = new Pagination([
            'route' => 'item/list',
            'lastKey' => $currentLastKey,
            'nextLastKey' => $nextLastKey,
            'pageSize' => $pageSize,
        ]);
        
        $this->assertEquals($links, $pagination->getLinks());
    }
}
