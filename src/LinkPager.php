<?php
/**
 * LinkPager class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

use yii\base\InvalidConfigException;
use yii\helpers\Html;
use yii\helpers\ArrayHelper;
use yii\web\Link;

/**
 * LinkPager extends \yii\widgets\LinkPager to displays a list of hyperlinks for
 * DynamoDB pagination.
 *
 * LinkPager should only be used with \UrbanIndo\Yii2\DynamoDb\Pagination object
 * that provides the current DynamoDb pagination key and the next pagination key.
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class LinkPager extends \yii\widgets\LinkPager
{
    /**
     * @var Pagination the pagination object that this pager is associated with.
     * You must set this property in order to make LinkPager work. This has to
     * be instance of \UrbanIndo\Yii2\DynamoDb\Pagination class.
     */
    public $pagination;
    
    /**
     * @var string|boolean the text label for the "first" page button. Note that this will NOT be HTML-encoded.
     * If it's specified as true, page number will be used as label.
     * Default is false that means the "first" page button will not be displayed.
     */
    public $firstPageLabel = true;
    
    /**
     * @var string|boolean the text label for the "current" page button. Note that this will NOT be HTML-encoded.
     * If it's specified as true, page number will be used as label.
     * Default is false that means the "current" page button will not be displayed.
     */
    public $currentPageLabel = false;
    
    /**
     * Initializes the pager.
     * @return void
     * @throws InvalidConfigException When the pagination is not instance of
     * \UrbanIndo\Yii2\DynamoDb\Pagination class.
     */
    public function init()
    {
        if (!$this->pagination instanceof Pagination) {
            throw new InvalidConfigException(
                'The "pagination" has to be instance of \UrbanIndo\Yii2\DynamoDb\Pagination.'
            );
        }
    }
    
    /**
     * Renders the page buttons.
     * @return string the rendering result
     */
    protected function renderPageButtons()
    {
        $buttons = [];
        $pagination = $this->pagination;
        
        $links = $pagination->getLinks();
        
        $currentUrl = ArrayHelper::getValue($links, Link::REL_SELF);
        
        if ($this->firstPageLabel !== false &&
                !empty($url = ArrayHelper::getValue($links, Pagination::LINK_FIRST)) &&
                $url != $currentUrl) {
            $buttons[] = $this->renderUrlPageButton($this->firstPageLabel, $url, $this->firstPageCssClass, false);
        }
        
        if ($this->prevPageLabel !== false) {
            $buttons[] = $this->renderUrlPageButton(
                $this->prevPageLabel,
                'javascript:history.back()',
                $this->prevPageCssClass,
                false
            );
        }
        
        if ($this->currentPageLabel !== false) {
            $buttons[] = $this->renderUrlPageButton($this->currentPageLabel, $currentUrl, null, true);
        }
        
        if ($this->nextPageLabel !== false && !empty($url = ArrayHelper::getValue($links, Pagination::LINK_NEXT))) {
            $buttons[] = $this->renderUrlPageButton($this->nextPageLabel, $url, $this->nextPageCssClass, false);
        }
        
        return Html::tag('ul', implode("\n", $buttons), $this->options);
    }
    
    /**
     * Renders a page button using URL.
     * You may override this method to customize the generation of page buttons.
     * @param string  $label    The text label for the button.
     * @param string  $url      The URL of the button.
     * @param string  $class    The CSS class for the page button.
     * @param boolean $disabled Whether to disable the button.
     * @return string the rendering result
     */
    protected function renderUrlPageButton($label, $url, $class, $disabled)
    {
        $options = ['class' => empty($class) ? $this->pageCssClass : $class];
        if ($disabled) {
            Html::addCssClass($options, $this->disabledPageCssClass);
            $options['data-href'] = $url;
            return Html::tag('li', Html::tag('span', $label), $options);
        }
        $linkOptions = $this->linkOptions;
        
        return Html::tag('li', Html::a($label, $url, $linkOptions), $options);
    }
}
