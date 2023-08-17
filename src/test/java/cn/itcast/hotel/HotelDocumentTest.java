package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.List;

@SpringBootTest
class HotelDocumentTest {

    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;

    private void handleResponse(SearchResponse response) {
        // 4.解析响应
        SearchHits searchHits = response.getHits();
        // 4.1.获取总条数
        long total = searchHits.getTotalHits().value;
        System.out.println("共搜索到" + total + "条数据");
        // 4.2.文档数组
        SearchHit[] hits = searchHits.getHits();
        // 4.3.遍历
        for (SearchHit hit : hits) {
            // 获取文档source
            String json = hit.getSourceAsString();
            // 反序列化
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            System.out.println("hotelDoc = " + hotelDoc);
        }
    }

    //  添加文档   添加数据到索引
    @Test
    void testAddDocument() throws IOException {
        // 1.从数据库中查询一条酒店的数据，数据库对应的POJO
        Hotel hotel = hotelService.getById(61083L);
        // 2.先创建一个ES对应的POJO，将数据放到对应的POJO中
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 3.转JSON
        String json = JSON.toJSONString(hotelDoc);

        //存储到ES中执行动作
        // 1.创建Request请求对象设置索引的名称，id：文档的唯一标识；设置要放到哪一个索引中
        IndexRequest request = new IndexRequest("hotel").id(hotelDoc.getId().toString());
        // 2.设置请求体，其实就是文档的JSON字符串
        request.source(json, XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }


    //查询文档
    @Test
    void testGetDocumentById() throws IOException {
        // 1.准备Request      // GET /hotel/_doc/{id}
        GetRequest request = new GetRequest("hotel", "61083");
        // 2.发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3.解析响应结果
        String json = response.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        //获取内容
        System.out.println("hotelDoc = " + hotelDoc);
    }


    //删除文档
    @Test
    void testDeleteDocumentById() throws IOException {
        // 1.准备Request      // DELETE /hotel/_doc/{id}
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        // 2.发送请求
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        //获取结果
        System.out.println(delete.getResult().toString());
    }


    //更新文档
    @Test
    void testUpdateById() throws IOException {
        // 1.准备Request
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备参数，每两个参数为一个键值对或json格式
        request.doc(
                "price", 870,
                "source",33
        );
        // 3.发送请求
        client.update(request, RequestOptions.DEFAULT);
    }


    //bulk：一个批次 最好控制在15MB只有------
    //批量添加数据到es中
    @Test
    void testBulkRequest() throws IOException {
        //开始时间
        long start = System.currentTimeMillis();

        // 从数据库查询所有的数据，循环遍历
        List<Hotel> list = hotelService.list();

        // 1.创建Request对象 相当于一个集合
        BulkRequest request = new BulkRequest();
        // 2.准备参数
        for (Hotel hotel : list) {
            // 2.1.创建一个ES对应的POJO，将数据放到对应的POJO中
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 2.2.转json
            String json = JSON.toJSONString(hotelDoc);
            // 2.3.   添加到bulk中
            // 设置文档的唯一标识(id)、请求体（文档数据）(source)
            request.add(new IndexRequest("hotel").id(hotel.getId().toString()).source(json, XContentType.JSON));
        }

        // 3.执行动作
        client.bulk(request, RequestOptions.DEFAULT);
        //结束时间
        long end = System.currentTimeMillis();

        System.out.println("花费了--->"+ (end-start));

    }

    @BeforeEach
    void setUp() {
        client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://localhost:9200")
        ));
    }


    //day2 查询所有
    @Test
    public void matchAll() throws IOException {
        //1、创建searchrequest对象
        SearchRequest request = new SearchRequest("hotel");
        //2、设置查询的条件（）
        request.source()
                //设置查询条件，查询所有
                .query(QueryBuilders.matchAllQuery());
        //3、执行搜索
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4、循环遍历
        //获取总条数
        long total = response.getHits().getTotalHits().value;
        System.out.println("total = " + total);
        //文档数组
        SearchHits hits = response.getHits();
        SearchHit[] hits1 = hits.getHits();
        for (SearchHit hit : hits1) {
            //获取文档source
            String sourceAsString = hit.getSourceAsString();
            //反序列化
            HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
            System.out.println("sourceAsString = " + hotelDoc);
        }
    }

        //match 匹配查询
    @Test
    public void testMatch() throws IOException {
        //1、准备request
        SearchRequest request = new SearchRequest("hotel");
        //2、准备DSL
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        //3、发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        //4、解析响应
        handleResponse(response);
    }

    //精确、范围查询  价格在100-500之间
    /*
    term : 词条精确匹配
    range : 范围查询
     */
    @Test
    public void testRang() throws IOException {
        //1、创建searchRequest对象
        SearchRequest request = new SearchRequest("hotel");
        //2、设置查询的条件 -- 范围查询
        request.source().query(
                QueryBuilders.rangeQuery("price").gte(100).lte(500)
                //QueryBuilders.rangeQuery("price").from(100,true).to(500,true)
        );
        //3、执行查询的动作
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    //term查询  查询深圳的酒店信息
    @Test
    public void testTerm() throws IOException {
        //1、创建searchRequest对象
        SearchRequest request = new SearchRequest("hotel");
        //2、设置查询条件  -- 不分词，整体进行匹配查询
        request.source().query(QueryBuilders.termQuery("city","深圳"));
        //3、执行查询的动作
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }



    /*
    #  bool查询：多个条件组合之后的查询
    #　MUST　  必须满足   and   文档匹配度提高了 需要算分排序，分值高的在前面
    # MUST_NOT   必须不满足   not
    # SHOULD   应该满足  or
    # FILTER   必须满足  and   文档匹配度降低了
    查询在深圳且价格在500-1000的酒店
     */
    @Test
    public void testBool() throws Exception{
        //1、创建searchRequest对象
        SearchRequest request = new SearchRequest("hotel");
        //2、设置查询条件
        //2.1、创建bool查询的对象
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        //2.2、创建条件
        TermQueryBuilder query1 = QueryBuilders.termQuery("city", "深圳");
        RangeQueryBuilder query2 = QueryBuilders.rangeQuery("price").gte(500).lte(1000);
        //2.3、组合条件
        boolQuery.must(query1).must(query2);
        request.source().query(boolQuery);
        //执行查询的动作
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }


    //排序和分页
    @Test
    public void testPageAndSort()throws Exception{
        //1、创建searchRequest对象
        SearchRequest request = new SearchRequest("hotel");
        //2、设置查询条件
        request.source().query(QueryBuilders.matchAllQuery());
        //3、设置分页条件、排序
        request.source().from(0).size(20);
        request.source().sort("price", SortOrder.ASC);
        //执行查询动作
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    //高亮
    //1、要有全文搜索（匹配查询） 2、设置高亮的字段，设置前缀、后缀
    //3、获取高亮之后的数据（带有标签的)
    @Test
    public void testHighLight()throws Exception{
        //1、创建searchRequest对象
        SearchRequest request = new SearchRequest("hotel");
        //2、设置查询条件 -- 匹配查询
        request.source().query(QueryBuilders.matchQuery("name","虹桥如家"));
        //3、设置高亮的字段和前缀、后缀
        request.source().highlighter(
                new HighlightBuilder().field("name")  //设置高亮的字段
                .requireFieldMatch(false)//设置不必须，高亮的字段不需要和查询的字段一致
                .preTags("<em style='color:yellow'>")//设置前缀
                .postTags("</em>")//设置后缀
        );
        //4、执行查询动作
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close();
    }
}
