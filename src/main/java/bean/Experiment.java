package bean;


import java.io.Serializable;

// ab 实验
public class Experiment implements Serializable {

    public String userId;
    public String domainName;
    public String experimentName;

    public Experiment() {
    }

    public Experiment(String userId, String domainName, String experimentName) {
        this.userId = userId;
        this.domainName = domainName;
        this.experimentName = experimentName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public String getExperimentName() {
        return experimentName;
    }

    public void setExperimentName(String experimentName) {
        this.experimentName = experimentName;
    }

    @Override
    public String toString() {
        return "Experiment{" +
                "userId='" + userId + '\'' +
                ", domainName='" + domainName + '\'' +
                ", experimentName='" + experimentName + '\'' +
                '}';
    }
}
