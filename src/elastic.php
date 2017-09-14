<?php

namespace Xdm\Elastic;

use Elasticsearch\ClientBuilder;
use ONGR\ElasticsearchDSL\Search;
use ONGR\ElasticsearchDSL\Sort\FieldSort;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery;
use ONGR\ElasticsearchDSL\Query\FullText\MatchQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\RangeQuery;
use ONGR\ElasticsearchDSL\Aggregation\Metric\SumAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Metric\AvgAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Metric\MinAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Metric\MaxAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Bucketing\TermsAggregation;
use ONGR\ElasticsearchDSL\Aggregation\Metric\ValueCountAggregation;

class Elastic{
    private $client;
    private $hosts = array();
    private $search;
    private $boolQuery;
    private $searchParams = array();
    private $size = 10000;

    /**
     * ElasticSearchService constructor.
     * @param array $hosts
     */
    public function __construct(array $hosts){
        $this->hosts = $hosts;
        $this->search = new Search();
        $this->boolQuery = new BoolQuery();
    }

    /**
     * 设置ES index
     * @param $database
     * @return $this
     */
    public function index($index){
        $this->searchParams['index'] = $index;
        return $this;
    }

    /**
     * 设置ES type
     * @param $table
     * @return $this
     */
    public function type($type){
        $this->searchParams['type'] = $type;
        return $this;
    }

    /**
     * 连接ES
     * @return $this
     */
    public function connect(){
        if(!$this->client){
            $this->client = ClientBuilder::create()->setHosts($this->hosts)->build();
        }
        return $this;
    }

    /**
     * ES where条件查询:where(['key' => value])
     * @param $condition
     * @return $this
     */
    public function where($condition){
        foreach($condition as $key => $value){
            $matchQuery = new MatchQuery($key, $value);
            $this->search->addQuery($matchQuery);
        }
        return $this;
    }

    /**
     * ES whereIn查询:whereIn('key', [value])
     * @param $key
     * @param $items
     * @return $this
     */
    public function whereIn($key, $items){
        $boolQuery = new BoolQuery();
        foreach ($items as $item){
            $matchQuery = new MatchQuery($key, $item);
            $boolQuery->add($matchQuery, BoolQuery::SHOULD);
        }
        $this->boolQuery->add($boolQuery, BoolQuery::MUST);
        return $this;
    }

    /**
     * ES范围查询[range('key', ['gte' => value1, 'lte' => value2])]
     * @param $key
     * @param $range
     * @return $this
     */
    public function range($key, $range){
        $rangeQuery = new RangeQuery($key, $range);
        $this->boolQuery->add($rangeQuery, BoolQuery::FILTER);
        return $this;
    }

    /**
     * ES统计查询
     * @param $field
     * @return $this
     */
    public function count($field){
        $valueCountAggregation = new ValueCountAggregation('count', $field);
        $this->search->addAggregation($valueCountAggregation);
        return $this;
    }

    /**
     * ES求和查询
     * @param $field
     * @return $this
     */
    public function sum($field){
        $sumAggregation = new SumAggregation($field, $field);
        $this->search->addAggregation($sumAggregation);
        return $this;
    }

    /**
     * ES最小值查询
     * @param $field
     * @return $this
     */
    public function min($field){
        $minAggregation = new MinAggregation($field, $field);
        $this->search->addAggregation($minAggregation);
        return $this;
    }

    /**
     * ES最大值查询
     * @param $field
     * @return $this
     */
    public function max($field){
        $maxAggregation = new MaxAggregation($field, $field);
        $this->search->addAggregation($maxAggregation);
        return $this;
    }

    /**
     * ES平均值查询
     * @param $field
     * @return $this
     */
    public function avg($field){
        $avgAggregation = new AvgAggregation($field, $field);
        $this->search->addAggregation($avgAggregation);
        return $this;
    }

    /**
     * 设置ES查询返回结果数量
     * @param $size
     * @return $this
     */
    public function take($size){
        $this->size = $size;
        return $this;
    }

    /**
     * ES分组查询
     * @param $field
     * @return $this
     */
    public function groupBy($field){
        $termsQuery = new TermsAggregation($field, $field);
        $termsQuery->addParameter('size', $this->size);
        $this->search->addAggregation($termsQuery);
        return $this;
    }

    /**
     * ES排序
     * @param $field
     * @param $order
     * @return $this
     */
    public function orderBy($field, $order){
        $sort = new FieldSort($field, $order);
        $this->search->addSort($sort);
        return $this;
    }

    /**
     * 提交ES查询
     * @return array
     */
    public function search(){
        $this->connect();
        $this->search->setSize($this->size);
        if(count($this->boolQuery->getQueries())){
            $this->search->addQuery($this->boolQuery);
        }
        $this->searchParams['body'] = $this->search->toArray();

        //提交查询
        $response = $this->client->search($this->searchParams);
        return $response;
    }
}