<?php
/**
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

    protected function prepareKeys($models)
    {
        
    }

    protected function prepareModels()
    {
        
    }

    protected function prepareTotalCount()
    {
        
    }

}
