package com.futhead.java.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * Created by Administrator on 2018/4/27.
 */
public class IndexSearch {

    public static void main(String[] args) throws Exception {
        search();
    }

    private static String serverUrl = "http://localhost:8080/solr/core1";

    public static void search()throws Exception{
        HttpSolrClient client = new  HttpSolrClient(serverUrl);

        //创建查询对象
        SolrQuery query = new SolrQuery();
        //q 查询字符串，如果查询所有*:*
        query.set("q", "张三");
        //fq 过滤条件，过滤是基于查询结果中的过滤
//        query.set("fq", "product_catalog_name:幽默杂货");
        //sort 排序，请注意，如果一个字段没有被索引，那么它是无法排序的
//        query.set("sort", "product_price desc");
        //start row 分页信息，与mysql的limit的两个参数一致效果
        query.setStart(0);
        query.setRows(10);
        //fl 查询哪些结果出来，不写的话，就查询全部，所以我这里就不写了
//        query.set("fl", "");
        //df 默认搜索的域
//        query.set("df", "product_keywords");

        //======高亮设置===
        //开启高亮
//        query.setHighlight(true);
        //高亮域
//        query.addHighlightField("product_name");
        //前缀
//        query.setHighlightSimplePre("<span style='color:red'>");
        //后缀
//        query.setHighlightSimplePost("</span>");


        //执行搜索
        QueryResponse queryResponse = client.query(query);
        //搜索结果
        SolrDocumentList results = queryResponse.getResults();
        //查询出来的数量
        long numFound = results.getNumFound();
        System.out.println("总查询出:" + numFound + "条记录");

        //遍历搜索记录
        //获取高亮信息
//        Map<String, Map<String, List<String>>> highlighting = queryResponse.getHighlighting();
        for (SolrDocument solrDocument : results) {
            System.out.println("name:" + solrDocument.get("name"));
            System.out.println("address:" + solrDocument.get("address"));
//            System.out.println("商品id:" + solrDocument.get("id"));
//            System.out.println("商品名称 :" + solrDocument.get("product_name"));
//            System.out.println("商品分类:" + solrDocument.get("product_catalog"));
//            System.out.println("商品分类名称:" + solrDocument.get("product_catalog_name"));
//            System.out.println("商品价格:" + solrDocument.get("product_price"));
//            System.out.println("商品描述:" + solrDocument.get("product_description"));
//            System.out.println("商品图片:" + solrDocument.get("product_picture"));
//
//            //输出高亮
//            Map<String, List<String>> map = highlighting.get(solrDocument.get("id"));
//            List<String> list = map.get("product_name");
//            if(list != null && list.size() > 0){
//                System.out.println(list.get(0));
//            }
        }

        client.close();
    }
}
