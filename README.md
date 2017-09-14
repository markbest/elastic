## 使用方法
- 1、composer require "xdm/elastic"
- 2、调用方法：
```
use Xdm\Elastic\Elastic;

//construct set host
$elastic = new Elastic($host);

//set index
$elastic->index($index);

//set type
$elastic->type($type);

//add where conditon filter
$elastic->where(['key' => 'value']);

//add whereIn conditon filter
$elastic->whereIn('key', ['value1', 'value2']);

//add range condition filter
$elastic->range('key', ['gte' => 'value1', 'lte' => 'value2']);

//set size
$elastic->take(100);

//add Aggregation
$elastic->count($field);
$elastic->groupBy($field);
$elastic->sum($field);
$elastic->max($field);
$elastic->min($field);
$elastic->avg($field);

//set order
$elastic->orderBy($field, 'desc');

//send query and get response
$response = $elastic->search();

```  