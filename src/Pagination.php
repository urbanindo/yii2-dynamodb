<?php
/**
 * Pagination class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use Yii;
use yii\web\Link;

/**
 * Pagination represents information relevant to pagination of data items.
 * @author Petra Barus <petra.barus@gmail.com>
 */
class Pagination extends \yii\data\Pagination
{
    /**
     * @var string name of the parameter storing the current page index.
     * @see params
     */
    public $lastKeyParam = 'last-key';
    
    /**
     * @var boolean whether to always have the last-key parameter in the URL created by [[createUrl()]].
     * If false and [[lastKey]] is null, the lastKey parameter will not be put in the URL.
     */
    public $forceLastKeyParam = true;
    
    /**
     * @var array parameters (name => value) that should be used to obtain the current page number
     * and to create new pagination URLs. If not set, all parameters from $_GET will be used instead.
     *
     * In order to add hash to all links use `array_merge($_GET, ['#' => 'my-hash'])`.
     *
     * The array element indexed by [[lastKeyParam]] is considered to be the current last key param (defaults to null).
     * while the element indexed by [[pageSizeParam]] is treated as the page size (defaults to [[defaultPageSize]]).
     */
    public $params;
    
    /**
     * This will only return 1.
     * @return integer number of pages
     */
    public function getPageCount()
    {
        return 1;
    }
    
    /**
     * Creates the URL suitable for pagination with the specified page number.
     * This method is mainly called by pagers when creating URLs used to perform pagination.
     * @param string  $lastKey  The key of the last evaluated item.
     * @param integer $pageSize The number of items on each page. If not set, the value of [[pageSize]] will be used.
     * @param boolean $absolute Whether to create an absolute URL. Defaults to `false`.
     * @return string the created URL
     * @see params
     * @see forcePageParam
     */
    public function createUrl($lastKey, $pageSize = null, $absolute = false)
    {
        $pageSize = (int) $pageSize;
        if (($params = $this->params) === null) {
            $request = Yii::$app->getRequest();
            $params = $request instanceof Request ? $request->getQueryParams() : [];
        }
        
        if ($lastKey !== null || $this->forcePageParam) {
            $params[$this->lastKeyParam] = $lastKey;
        } else {
            unset($params[$this->lastKeyParam]);
        }
        if ($pageSize <= 0) {
            $pageSize = $this->getPageSize();
        }
        if ($pageSize != $this->defaultPageSize) {
            $params[$this->pageSizeParam] = $pageSize;
        } else {
            unset($params[$this->pageSizeParam]);
        }
        
        $params[0] = $this->route === null ? Yii::$app->controller->getRoute() : $this->route;
        $urlManager = $this->urlManager === null ? Yii::$app->getUrlManager() : $this->urlManager;
        if ($absolute) {
            return $urlManager->createAbsoluteUrl($params);
        } else {
            return $urlManager->createUrl($params);
        }
    }
    
    /**
     * Returns just one link to the next page.
     * @param boolean $absolute Whether the generated URLs should be absolute.
     * @return array array containing links to the next page.
     */
    public function getLinks($absolute = false)
    {
        $pageSize = $this->getPageSize();
        $links = [
            Link::REL_SELF => $this->createUrl(
                $this->getLastKey(),
                $pageSize,
                $absolute
            )
        ];
        
        $links[self::LINK_FIRST] = $this->createUrl(null, $pageSize, $absolute);
        
        if (($nextLastKey = $this->getNextLastKey()) !== null) {
            $links[self::LINK_NEXT] = $this->createUrl($nextLastKey, $pageSize, $absolute);
        }
        return $links;
    }
    
    /**
     * @return integer the limit of the data. This may be used to set the
     * LIMIT value for Query or Scan operation.
     * Note that if the page size is infinite, a value -1 will be returned.
     */
    public function getLimit()
    {
        $pageSize = $this->getPageSize();

        return $pageSize < 1 ? -1 : $pageSize;
    }
    
    /**
     * Stores the current last key.
     * @var string|string[]
     */
    private $_lastKey;
    
    /**
     * Returns the last key evaluated in the DynamoDB.
     * @return string|string[]
     */
    public function getLastKey()
    {
        if ($this->_lastKey === null) {
            $this->setLastKey($this->getQueryParam($this->lastKeyParam));
        }
        return $this->_lastKey;
    }
    
    /**
     * Sets the current last key.
     * @param string|string[] $value The last key that was evaluated by DynamoDB.
     * @return void
     */
    public function setLastKey($value)
    {
        $this->_lastKey = $value;
    }
    
    /**
     * Stores the next last key.
     * @var string|string[]
     */
    private $_nextLastKey;
    
    /**
     * Returns the next last key.
     * @return string|string[]
     */
    public function getNextLastKey()
    {
        return $this->_nextLastKey;
    }
    
    /**
     * Sets the next last key. This has to be set manually in the data provider.
     * @param string|string[] $value The last key that was evaluated by DynamoDB.
     * @return void
     */
    public function setNextLastKey($value)
    {
        $this->_nextLastKey = $value;
    }
    
    /**
     * This is shorthand for `getLastKey()`.
     * @return string
     */
    public function getOffset()
    {
        return $this->getLastKey();
    }
}
