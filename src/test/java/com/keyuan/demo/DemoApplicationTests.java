package com.keyuan.demo;

import com.alibaba.fastjson.JSON;
import com.keyuan.demo.domain.Goods;
import com.keyuan.demo.enums.FieldsEnum;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;

@SpringBootTest
class DemoApplicationTests {

    @Autowired
    private RestHighLevelClient restHighLevelClient;


    /**
     * 创建index
     *
     * @return
     * @date 2020-08-12 17:20
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void creatGoodIndex(){
        Set<String> set = new HashSet<>();
        set.add("name");
        set.add("colorName");
        createIndex(Goods.class,"goods",set);
    }

    /**
     * 删除索引
     *
     * @return
     * @date 2020-08-12 17:18
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void deleteIndex(){
        deleteAll("test");
    }

    /**
     * 删除索引
     *
     * @return
     * @date 2020-08-12 17:18
     * @author huangkeyuan@leimingtech.com
     **/
    private void deleteAll(String index){
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(index);
        try {
            restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义索引
     *
     * @return
     * @date 2020-08-12 17:12
     * @author huangkeyuan@leimingtech.com
     **/
    private void createIndex(Class aClass,String index, Set<String> segmentationFileds ){

        // 获取所有的字段
        Field[] declaredFields = aClass.getDeclaredFields();

        // 映射
        XContentBuilder mapping;
        // 设置
        XContentBuilder setting;

        try {
            mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
            for (Field declaredField : declaredFields) {
                FieldsEnum type = getType(declaredField);
                String name = declaredField.getName();
                if(segmentationFileds.contains(name) && FieldsEnum.TEXT.equals(type)){
                    mapping.startObject(name);
                    mapping.field("type",type.value())
                            .field("analyzer", "ik-index")
                            .field("search_analyzer", "ik-search");
                    mapping.startObject("fields")
                            .startObject("keyword")
                            .field("type","keyword")
                            .field("ignore_above",512)
                            .endObject()
                            .endObject();
                    mapping.endObject();
                }else if(FieldsEnum.TEXT.equals(type)){
                    mapping.startObject(name);
                    mapping.field("type",type.value());
                    mapping.startObject("fields")
                            .startObject("keyword")
                            .field("type","keyword")
                            .field("ignore_above",512)
                            .endObject()
                            .endObject();
                    mapping.endObject();
                }else if (FieldsEnum.DATE.equals(type)) {
                    mapping.startObject(name);
                    mapping.field("type", type.value()).field("format", "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis").endObject();
                } else if(FieldsEnum.LONG.equals(type) || FieldsEnum.DOUBLE.equals(type)){
                    mapping.startObject(name)
                            .field("type", type.value())
                            .endObject();
                }
            }
            mapping.endObject().endObject();
            mapping.close();
            System.out.println(mapping.toString());
            //添加setting
            setting = XContentFactory.jsonBuilder().startObject();
            // 设置分片数
            setting.field("number_of_shards",6);
            // 设置备份数
            setting.field("number_of_replicas",1);

            setting.startObject("analysis");
//			setting.startObject("filter");
            //同义词的过滤器
//			setting.startObject("net_synonym").field("type", "dynamic_synonym")
//					.field("synonyms_path", SYNONYMS_PATH)
//					.field("interval", 60).endObject();
//			setting.endObject();
            //ik分词器、同义词、
            setting.startObject("analyzer");
            setting.startObject("ik-index").field("type", "custom")
                    .field("tokenizer", "ik_max_word")
//					.field("filter", new String[]{"net_synonym"})
                    .field("char_filter", new String[]{"html_strip"}).endObject();
            setting.startObject("ik-search").field("type", "custom")
                    .field("tokenizer", "ik_smart")
//					.field("filter", new String[]{"net_synonym"})
                    .field("char_filter", new String[]{"html_strip"}).endObject();
            setting.endObject();
            setting.endObject().endObject();
            setting.close();
            CreateIndexRequest request = new CreateIndexRequest(index);
            request.settings(setting).mapping(mapping);
            restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 数据类型转换
     *
     * @return
     * @date 2020-08-12 17:22
     * @author huangkeyuan@leimingtech.com
     **/
    private FieldsEnum getType(Field field){
        String ctype = "text";
        //字段类型判断
        boolean isLong = field.getType().getName().equalsIgnoreCase("java.lang.Long");
        boolean isInteger = field.getType().getName().equalsIgnoreCase("java.lang.Integer") || field.getType().getName().equalsIgnoreCase("int");
        boolean isString = field.getType().getName().equalsIgnoreCase("java.lang.String");
        boolean isList = field.getType().getName().equalsIgnoreCase("java.util.list");
        boolean isDate = field.getType().getName().equalsIgnoreCase("java.util.Date");
        boolean isdouble = field.getType().getName().equalsIgnoreCase("java.lang.Double");
        boolean isBigDecimal = field.getType().getName().equalsIgnoreCase("java.math.BigDecimal");
        if (isLong || isInteger) {
            return FieldsEnum.LONG;
        }
        if (isdouble || isBigDecimal) {
            return FieldsEnum.DOUBLE;
        }
        if (isString) {
            return FieldsEnum.TEXT;
        }
        if(isDate){
            return FieldsEnum.DATE;
        }
        return FieldsEnum.OTHER;
    }

    @Test
    public void save() {
        // 删除索引
//		deleteAll("goods");

//		Set<String> set = new HashSet<>();
//		set.add("name");
//		set.add("colorName");
//		createIndex(Goods.class,"goods",set);

        // 获取数据
        List<Map<String, Object>> date = getDate();
        // 单个插入
		date.forEach(data -> {
			saveDate("goods",data);
		});
        // 批量插入
//        saveDateAll("goods",date);

    }

    /**
     * 保存单条数据
     *
     * @return
     * @date 2020-08-12 17:23
     * @author huangkeyuan@leimingtech.com
     **/
    private void saveDate(String index,Map<String, Object> data){
        IndexRequest indexRequest = new IndexRequest(index).source(data).id(data.get("id").toString());
        try {
            System.out.println(indexRequest.toString());
            restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量保存数据
     *
     * @return
     * @date 2020-08-12 17:23
     * @author huangkeyuan@leimingtech.com
     **/
    private void saveDateAll(String index,List<Map<String, Object>> datas){
        if(!CollectionUtils.isEmpty(datas)){
            BulkRequest bulkRequest = new BulkRequest();

            datas.forEach(data->{
                IndexRequest indexRequest = new IndexRequest(index).source(data).id(data.get("id").toString());
                bulkRequest.add(indexRequest);
            });

            try {
                System.out.println(bulkRequest.toString());
                restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private List<Map<String,Object>> getDate(){
        List<Map<String,Object>> result = new ArrayList<>();
        Goods goods = new Goods("00001","袜子",new BigDecimal(21.3D),1L,1L,1L,"红色");
        String s = JSON.toJSONString(goods);
        Map map = JSON.parseObject(s, Map.class);
        result.add(map);
        goods = new Goods("00002","袜子",new BigDecimal(21.3D),2L,1L,2L,"白色");
        s = JSON.toJSONString(goods);
        map = JSON.parseObject(s, Map.class);
        result.add(map);
        goods = new Goods("00003","袜子",new BigDecimal(21.3D),3L,1L,3L,"黑色");
        s = JSON.toJSONString(goods);
        map = JSON.parseObject(s, Map.class);
        result.add(map);
        goods = new Goods("00004","裤子牛仔裤直筒休闲",new BigDecimal(50.3D),4L,2L,1L,"红色");
        s = JSON.toJSONString(goods);
        map = JSON.parseObject(s, Map.class);
        result.add(map);
        goods = new Goods("00005","裤子牛仔裤直筒休闲",new BigDecimal(50.3D),5L,2L,2L,"白色");
        s = JSON.toJSONString(goods);
        map = JSON.parseObject(s, Map.class);
        result.add(map);
        return result;
    }

    /**
     * 功能描述: 判断索引是否存在
     *
     * @return
     * @date 2020-08-12 17:56
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void indexExist(){
        // goods中的"00001"是否存在
        GetIndexRequest goods = new GetIndexRequest("goods");
        try {
            boolean exists = restHighLevelClient.indices().exists(goods, RequestOptions.DEFAULT);
            System.out.println("goods索引存在与否:"+exists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新数据
     *
     * @return
     * @date 2020-08-12 17:59
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void update(){
        Map<String,Object> param = new HashMap<>();
        param.put("name","裤子");
        UpdateRequest goods = new UpdateRequest("goods", "00001").doc(param);
        try {
            restHighLevelClient.update(goods,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据索引和id删除
     *
     * @return
     * @date 2020-08-12 18:00
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void delete(){
        DeleteRequest goods = new DeleteRequest("goods","00001");
        try {
            restHighLevelClient.delete(goods,RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断索引下某个id是否存在
     *
     * @return
     * @date 2020-08-12 18:00
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void indexIdExist(){
        // goods中的"00001"是否存在
        GetRequest goods = new GetRequest("goods","00001");
        try {
            boolean exists = restHighLevelClient.exists(goods, RequestOptions.DEFAULT);
            System.out.println("goods索引存在00001与否:"+exists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getById(){
        GetRequest goods = new GetRequest("goods", "00003");
        GetResponse documentFields = null;
        try {
            documentFields = restHighLevelClient.get(goods, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sourceAsString = documentFields.getSourceAsString();
        Goods goods1 = JSON.parseObject(sourceAsString, Goods.class);
        System.out.println(goods1.toString());

    }

    /**
     * 分页查询
     *
     * @return
     * @date 2020-08-12 18:04
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchPage(){
        SearchRequest goods = new SearchRequest("goods");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(2).from(0);

        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 排序
     *
     * @return
     * @date 2020-08-12 18:08
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchSort(){
        SearchRequest goods = new SearchRequest("goods");

        //searchSourceBuilder.sort(SortBuilders.fieldSort("skuId").order(SortOrder.DESC));
        //searchSourceBuilder.sort(SortBuilders.fieldSort("skuId").order(SortOrder.ASC));
        //searchSourceBuilder.sort("skuId",SortOrder.DESC);
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "裤子");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.sort(SortBuilders.scoreSort().order(SortOrder.DESC));
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 相等的查询
     *
     * @return
     * @date 2020-08-12 18:10
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchTerm(){
        SearchRequest goods = new SearchRequest("goods");


        // TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "袜子");
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("colorId", 1L);
        //TermQueryBuilder termQueryBuilder = new TermQueryBuilder("colorId", 1L);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(termQueryBuilder);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * in查询
     *
     * @return
     * @date 2020-08-12 18:10
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchTerms(){
        SearchRequest goods = new SearchRequest("goods");

        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("skuId", new long[]{1L,2L});

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(termsQueryBuilder);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * like
     *
     * @return
     * @date 2020-08-12 18:10
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchWildcard(){
        SearchRequest goods = new SearchRequest("goods");

        WildcardQueryBuilder name = QueryBuilders.wildcardQuery("name.keyword", "*子牛仔*");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(name);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 范围查询
     *
     * @return
     * @date 2020-08-12 18:11
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchRange(){
        SearchRequest goods = new SearchRequest("goods");

        // 范围
        //RangeQueryBuilder skuIds = QueryBuilders.rangeQuery("skuId").gte(1L).lte(1L);
        // >=
        //RangeQueryBuilder skuIds = QueryBuilders.rangeQuery("skuId").gte(1L);
        // >
        //RangeQueryBuilder skuIds = QueryBuilders.rangeQuery("skuId").gt(1L);
        // <=
        //RangeQueryBuilder skuIds = QueryBuilders.rangeQuery("skuId").lte(2L);
        // <
        RangeQueryBuilder skuIds = QueryBuilders.rangeQuery("skuId").lt(2L);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(skuIds);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * match查询，带分词
     *
     * @return
     * @date 2020-08-12 18:11
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchMatch(){
        SearchRequest goods = new SearchRequest("goods");

        //MatchQueryBuilder name = QueryBuilders.matchQuery("name", "直筒");
        MatchQueryBuilder name = QueryBuilders.matchQuery("name", "裤子直筒");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(name);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 全文检索
     *
     * @return
     * @date 2020-08-12 18:12
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchMatchAll(){
        SearchRequest goods = new SearchRequest("goods");
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("裤子红色","name","colorName");
        // 增加权重
//		Map<String,Float> fields = new HashMap<>();
//		fields.put("name",1.0F);
//		fields.put("colorName",2.0F);
//		MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("裤子红色").fields();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(multiMatchQueryBuilder);
        System.out.println(searchSourceBuilder);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * bool查询,and条件
     *
     * @return
     * @date 2020-08-12 18:14
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchBoolMust(){
        SearchRequest goods = new SearchRequest("goods");


        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("colorId", 1L);
        WildcardQueryBuilder name = QueryBuilders.wildcardQuery("name.keyword", "*裤子*");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(termQueryBuilder).must(name);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *  bool查询,or条件
     *
     * @return
     * @date 2020-08-12 18:14
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchBoolShould(){
        SearchRequest goods = new SearchRequest("goods");


        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("colorId", 1L);
        WildcardQueryBuilder name = QueryBuilders.wildcardQuery("name.keyword", "*裤子*");
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(termQueryBuilder).should(name);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *  过滤和搜索的对比
     *
     * @return
     * @date 2020-08-12 18:15
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchFilter(){

        SearchRequest goods = new SearchRequest("goods");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "裤子");
        boolQueryBuilder.filter(matchQueryBuilder);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);
        //searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.explain(true);
        searchSourceBuilder.sort(SortBuilders.scoreSort());
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 搜索结果用指定字段展示
     *
     * @return
     * @date 2020-08-12 18:17
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchIncluds(){
        SearchRequest goods = new SearchRequest("goods");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"name","skuId"},null);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 折叠
     *
     * @return
     * @date 2020-08-12 18:18
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchCollapse(){
        SearchRequest goods = new SearchRequest("goods");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        CollapseBuilder spuId = new CollapseBuilder("spuId");
        searchSourceBuilder.collapse(spuId);
        SearchRequest source = goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(source, RequestOptions.DEFAULT);
            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 权重
     *
     * @return
     * @date 2020-08-12 18:21
     * @author huangkeyuan@leimingtech.com
     **/
    @Test
    public void searchBoost(){
        SearchRequest goods = new SearchRequest("goods");

        // 构建条件
        TermQueryBuilder skuId = QueryBuilders.termQuery("skuId", 5L).boost(12.0F);
        TermQueryBuilder colorId = QueryBuilders.termQuery("colorId", 1L).boost(6.0F);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.should(skuId).should(colorId);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.sort(SortBuilders.scoreSort());

        System.out.println(searchSourceBuilder);
        goods.source(searchSourceBuilder);
        try {
            SearchResponse search = restHighLevelClient.search(goods, RequestOptions.DEFAULT);

            long total = search.getHits().getTotalHits().value;
            System.out.println(total);
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit : hits) {
                String sourceAsString = hit.getSourceAsString();
                System.out.println(sourceAsString);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
