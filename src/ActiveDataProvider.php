<?php
/**
 * ActiveDataProvider class file.
 * @author Petra Barus <petra.barus@gmail.com>
 */

namespace UrbanIndo\Yii2\DynamoDb;

/**
 * ActiveDataProvider implements a data provider based on DynamoDB Query and ActiveQuery.
 *
 * ActiveDataProvider provides data by performing DB queries using [[query]].
 *
 * The following is an example of using ActiveDataProvider to provide ActiveRecord instances:
 *
 * ```php
 * $provider = new ActiveDataProvider([
 *     'query' => Post::find(),
 *     'pagination' => [
 *         'pageSize' => 20,
 *     ],
 * ]);
 *
 * // get the posts in the current page
 * $posts = $provider->getModels();
 * ```
 *
 * @author Petra Barus <petra.barus@gmail.com>
 */
class ActiveDataProvider extends \yii\data\BaseDataProvider
{

    /**
     * Prepares the keys associated with the currently available data models.
     * @param array $models The available data models.
     * @return array the keys.
     */
    protected function prepareKeys(array $models)
    {
        $models;
        return [];
    }

    /**
     * Prepares the data models that will be made available in the current page.
     * @return array the available data models
     */
    protected function prepareModels()
    {
        return [];
    }

    /**
     * Returns a value indicating the total number of data models in this data provider.
     * @return integer total number of data models in this data provider.
     */
    protected function prepareTotalCount()
    {
        return 0;
    }
}
