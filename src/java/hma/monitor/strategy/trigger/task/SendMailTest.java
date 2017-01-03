package hma.monitor.strategy.trigger.task;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.SimpleEmail;

/**
 * Created by weizhonghui on 2016/11/19.
 */
public class SendMailTest {
    private void doSendMail(String[] receivers, String subject, String body) {

        try {
            //String hostname = "yz750.hadoop.data.sina.com.cn";
            SimpleEmail email = new SimpleEmail();
            for (int i = 0; i < receivers.length; i++){
                email.addTo(receivers[i]);
            }
            email.setCharset("gbk");
            //String mfrom = System.getProperty("user.name") + "@"+ hostname;
            String mfrom = "monitor@hma.org";
            System.out.println("log : " + mfrom);
            email.setFrom(mfrom);
            email.setAuthentication("boyan5@staff.sina.com.cn", "ddzs%%017");

            email.setHostName("mail.staff.sina.com.cn");
            email.setSubject(subject);
            email.setMsg(body);
            email.send();
        } catch (EmailException ee) {
            String tmp = "";
            for (int i = 0; i < receivers.length; i++){
                tmp = tmp + receivers[i];
            }
            System.err.println("EmailException: mail is = " + tmp);
            throw new RuntimeException(ee);
        }

    }
    public static void main(String [] args ){
        String [] receivers = {"boyan5@staff.sina.com.cn"};
        String subject = "Hma EmailSend Experiment";
        String body = "Last week I went to the threatre. I had a very good seat and the play was very interesting.";
        new SendMailTest().doSendMail(receivers,subject,body);
    }
}

