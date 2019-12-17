package bean;

import java.io.Serializable;

// 用户行为数据
public class UserAction implements Serializable {
    public String userId;
    public String articleId;
    public String action;


    public UserAction() {
    }

    public UserAction(String userId, String articleId, String action) {
        this.userId = userId;
        this.articleId = articleId;
        this.action = action;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId='" + userId + '\'' +
                ", articleId='" + articleId + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}