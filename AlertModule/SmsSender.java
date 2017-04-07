import com.twilio.Twilio;
import com.twilio.rest.api.v2010.account.Message;
import com.twilio.type.PhoneNumber;
import java.net.URISyntaxException;

public class SmsSender {

    public static void sendSMS(String ACCOUNT_SID, String AUTH_TOKEN, 
                        String to_Number, String from_Number, String sms_message){
        Twilio.init(ACCOUNT_SID, AUTH_TOKEN);

        Message message = Message
                .creator(new PhoneNumber(to_Number),  // to
                         new PhoneNumber(from_Number),  // from
                         sms_message)
                .create();
    }
}
