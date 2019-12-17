package bean;

import java.io.Serializable;

// 统计结果
public class ActionStat implements Serializable {
    public String userId;
    public Long count;

    public ActionStat() {
    }

    public ActionStat(String userId, Long count) {
        this.userId = userId;
        this.count = count;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ActionStat{" +
                "userId='" + userId + '\'' +
                ", count=" + count +
                '}';
    }
}
