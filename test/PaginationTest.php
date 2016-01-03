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
            [
                ['a', 'f'],
                null,
                '/index.php?r=item%2Flist&last-key%5B0%5D=a&last-key%5B1%5D=f',
            ],
            [
                ['a', 'f'],
                5,
                '/index.php?r=item%2Flist&last-key%5B0%5D=a&last-key%5B1%5D=f&per-page=5',
            ],
            [
                ['attr1' => 'a', 'attr2' => 'f'],
                5,
                '/index.php?r=item%2Flist&last-key%5Battr1%5D=a&last-key%5Battr2%5D=f&per-page=5',
            ],
        ];
    }

    /**
     * @dataProvider dataProviderCreateUrl
     *
     * @param string|string[] $lastKey
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
                    'first' => '/index.php?r=item%2Flist',
                    'self' => '/index.php?r=item%2Flist',
                    'next' => '/index.php?r=item%2Flist&last-key=5',
                ]
            ],
            [
                5,
                10,
                null,
                [
                    'first' => '/index.php?r=item%2Flist',
                    'self' => '/index.php?r=item%2Flist&last-key=5',
                    'next' => '/index.php?r=item%2Flist&last-key=10',
                ]
            ],
            [
                5,
                10,
                10,
                [
                    'first' => '/index.php?r=item%2Flist&per-page=10',
                    'self' => '/index.php?r=item%2Flist&last-key=5&per-page=10',
                    'next' => '/index.php?r=item%2Flist&last-key=10&per-page=10',
                ]
            ],
            [
                ['a', 'b'],
                ['a', 'f'],
                10,
                [
                    'first' => '/index.php?r=item%2Flist&per-page=10',
                    'self' => '/index.php?r=item%2Flist&last-key%5B0%5D=a&last-key%5B1%5D=b&per-page=10',
                    'next' => '/index.php?r=item%2Flist&last-key%5B0%5D=a&last-key%5B1%5D=f&per-page=10',
                ]
            ],
            [
                ['attr1' => 'a', 'attr2' => 'b'],
                ['attr1' => 'a', 'attr2' => 'f'],
                10,
                [
                    'first' => '/index.php?r=item%2Flist&per-page=10',
                    'self' => '/index.php?r=item%2Flist&last-key%5Battr1%5D=a&last-key%5Battr2%5D=b&per-page=10',
                    'next' => '/index.php?r=item%2Flist&last-key%5Battr1%5D=a&last-key%5Battr2%5D=f&per-page=10',
                ]
            ]
        ];
    }

    /**
     * @dataProvider dataProviderGetLinks
     *
     * @param string|string[] $currentLastKey The current last key.
     * @param string|string[] $nextLastKey    The next last key.
     * @param integer         $pageSize       The page size to show.
     * @param array           $links          The links resulted
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
