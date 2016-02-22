package info.xiaohei.www.mr.kpi;

/**
 * Created by xiaohei on 16/2/20.
 *
 * kpi实体类,每个kpi对象包含了对应日志的每个属性
 * 提供parse方法对每行的日志进行解析
 * 重写toString方法
 * main方法中提供了demo数据进行测试
 */
public class Kpi {

    //判断该记录格式是否正确
    private Boolean is_validate = true;
    //需要解析的属性为8个
    private String remote_addr;//用户ip,0
    private String remote_user;//客户端用户名,1
    private String request_time;//请求时间,3
    private String request_method;//请求方法,5
    private String request_page;// 请求页面,6
    private String request_http;// http协议信息,7
    private String request_status;//返回的状态码,8
    private String sent_bytes;//发送的页面字节数,9
    private String http_referrer;//从什么页面跳转进来,10
    private String user_agent;//用户使用的客户端信息,数组剩下的部分

    /**
     * 解析每行日志数据,包括验证数据的合法性,将各个所需要的属性填充到kpi对象中
     * @param line 一行日志数据
     * @return 返回的kpi对象包含了该行日志的所有信息
     * */
    public static Kpi parse(String line) {
        String[] arr = line.split(" ");
        Kpi kpi = new Kpi();
        //标准数据格式为23个元素
        if (arr.length > 22) {
            //根据每行日志的数据位置进行解析
            kpi.setRemote_addr(arr[0]);
            kpi.setRemote_user(arr[1]);
            kpi.setRequest_time(arr[3].substring(1));
            kpi.setRequest_method(arr[5].substring(1));
            kpi.setRequest_page(arr[6]);
            if (arr[7].length() < 1) {
                kpi.setRequest_http("http");
            } else {
                kpi.setRequest_http(arr[7].substring(0, arr[7].length() - 1));
            }
            kpi.setRequest_status(arr[8]);
            kpi.setSent_bytes(arr[9]);
            kpi.setHttp_referrer(arr[10]);
            kpi.setUser_agent(arr[11]);

            //过滤响应码>400的错误请求
            if (!kpi.getRequest_status().equals("") && Integer.parseInt(kpi.getRequest_status()) > 400) {
                kpi.setIs_validate(false);
            }
        } else {
            kpi.setIs_validate(false);
        }
        return kpi;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("valid:").append(this.is_validate);
        sb.append("\nremote_addr:").append(this.remote_addr);
        sb.append("\nremote_user:").append(this.remote_user);
        sb.append("\nrequest_time:").append(this.request_time);
        sb.append("\nrequest_method:").append(this.request_method);
        sb.append("\nrequest_http:").append(this.request_http);
        sb.append("\nrequest_page:").append(this.request_page);
        sb.append("\nrequest_status:").append(this.request_status);
        sb.append("\nuser_agent:").append(this.user_agent);
        sb.append("\nhttp_referrer:").append(this.http_referrer);
        sb.append("\nsent_bytes:").append(this.sent_bytes);
        return sb.toString();
    }

    public static void main(String[] args) {
        //数据格式:
        String line = "66.102.12.84 - - " +
                "[04/Jan/2012:23:18:32 +0800] " +
                "\"GET /ctp080113.php?tid=1495366 HTTP/1.1\" " +
                "200 " +
                "31 " +
                "\"http://www.itpub.net/thread-1495366-1-1.html\" " +
                "\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/534.51 (KHTML, like Gecko; Google Web Preview) Chrome/12.0.742 Safari/534.51\"";
        Kpi kpi = Kpi.parse(line);
        System.out.println(kpi.toString());
    }

    public String getRemote_addr() {
        return remote_addr;
    }

    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }

    public String getRemote_user() {
        return remote_user;
    }

    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }

    public String getRequest_time() {
        return request_time;
    }

    public void setRequest_time(String request_time) {
        this.request_time = request_time;
    }

    public String getRequest_status() {
        return request_status;
    }

    public void setRequest_status(String request_status) {
        this.request_status = request_status;
    }

    public String getSent_bytes() {
        return sent_bytes;
    }

    public void setSent_bytes(String sent_bytes) {
        this.sent_bytes = sent_bytes;
    }

    public String getHttp_referrer() {
        return http_referrer;
    }

    public void setHttp_referrer(String http_referrer) {
        this.http_referrer = http_referrer;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public Boolean getIs_validate() {
        return is_validate;
    }

    public void setIs_validate(Boolean is_validate) {
        this.is_validate = is_validate;
    }

    public String getRequest_method() {
        return request_method;
    }

    public void setRequest_method(String request_method) {
        this.request_method = request_method;
    }

    public String getRequest_page() {
        return request_page;
    }

    public void setRequest_page(String request_page) {
        this.request_page = request_page;
    }

    public String getRequest_http() {
        return request_http;
    }

    public void setRequest_http(String request_http) {
        this.request_http = request_http;
    }
}
