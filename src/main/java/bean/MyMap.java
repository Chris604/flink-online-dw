package bean;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

// 自定义 map 函数
public class MyMap implements MapFunction<String, UserAction> {

    @Override
    public UserAction map(String value) throws Exception {

        JSONObject jsonObject = JSONObject.parseObject(value);
        JSONObject content = JSONObject.parseObject(jsonObject.getString("content"));
        if (jsonObject.getString("content") != null) {
            JSONObject properties = JSONObject.parseObject(content.getString("properties"));
            String userId = properties.getString("userId");
            String articleId = properties.getString("article_id");
            String action = content.getString("event");

            UserAction us = new UserAction(userId, articleId, action);
            return us;
        }
        return null;
    }

    public static class ExpMap implements MapFunction<String, Experiment> {
        Experiment experiment = new Experiment();

        @Override
        public Experiment map(String value) throws Exception {
            JSONObject jsonObject = JSONObject.parseObject(value);
            experiment.userId = jsonObject.getString("userid");
            experiment.domainName = jsonObject.getString("domain_name");
            experiment.experimentName = jsonObject.getString("experiment_name");

            return experiment;
        }
    }
}
