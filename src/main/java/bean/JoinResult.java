package bean;

import java.io.Serializable;

// 两条流 join 的结果
public class JoinResult implements Serializable {

    public String userId;
    public String articleId;
    public String action;
    public String expName;

    public JoinResult() {
    }

    public JoinResult(String userId, String articleId, String action, String expName) {
        this.userId = userId;
        this.articleId = articleId;
        this.action = action;
        this.expName = expName;
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

    public String getExpName() {
        return expName;
    }

    public void setExpName(String expName) {
        this.expName = expName;
    }

    @Override
    public String toString() {
        return "JoinResult{" +
                "userId='" + userId + '\'' +
                ", articleId='" + articleId + '\'' +
                ", action='" + action + '\'' +
                ", expName='" + expName + '\'' +
                '}';
    }
}
