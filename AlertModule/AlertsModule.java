import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.*;

import java.io.File;
import java.io.PrintWriter;
import java.io.FileOutputStream;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class AlertsModule{

    private static String Alerts_Topic;
    private static String Alerts_Index;
    private static String ACCOUNT_SID;
    private static String AUTH_TOKEN;
    private static String Twilio_Phone;
    private static String To_Phone;

    private static String Email_ID;
    private static String Password;
    private static String To_Email;
    private static String maprFile;

    private static String SendToEmail="No";
    private static String SendToMobile="No";
    private static String SendToDB="No"; 
    private static Boolean DEBUG=false;
    private static String SendToMapr="No";

    public static KafkaConsumer<String,String> consumer;

    @SuppressWarnings("unchecked")
    public static void main(String args[])
    {
        //Read the properties file
        Properties prop= new Properties();
        InputStream input = null;

        try{
            input = new FileInputStream("config.properties");
            prop.load(input);

            Alerts_Topic=prop.getProperty("alerts_topic");
            Alerts_Index=prop.getProperty("alerts_index");

            ACCOUNT_SID=prop.getProperty("ACCOUNT_SID");
            AUTH_TOKEN=prop.getProperty("AUTH_TOKEN");
            Twilio_Phone=prop.getProperty("twilio_phone");
            To_Phone=prop.getProperty("to_phone");

            Email_ID=prop.getProperty("email_id");
            Password=prop.getProperty("password");
            To_Email=prop.getProperty("to_email");

            SendToEmail=prop.getProperty("SendToEmail");
            SendToMobile=prop.getProperty("SendToMobile");
            SendToDB=prop.getProperty("SendToDB");
            SendToMapr=prop.getProperty("SendToMapR");

            PrintWriter filewriter =null;      
            if (SendToMapr.equalsIgnoreCase("Yes")){
               maprFile=prop.getProperty("mapr_file");
               //#filewriter = new PrintWriter(new File(maprFile));
               filewriter = new PrintWriter(new FileOutputStream( new File(maprFile), true ));
               System.out.println("MapR file: "+maprFile);
            }
           
            if ( prop.getProperty("DEBUG").equalsIgnoreCase("Yes")){
              DEBUG=true;
            }

            if (DEBUG){
               System.out.println("Alerts topic: "+Alerts_Topic);
               System.out.println("Alerts index in elasticsearch : "+Alerts_Index);
               System.out.println("ACCOUNT_SID: "+ACCOUNT_SID);
               System.out.println("ACCOUNT_TOKEN: "+AUTH_TOKEN);
               System.out.println("Twilio Phone: "+Twilio_Phone);
               System.out.println("Phone to send message: "+To_Phone);
               System.out.println("CPFOODS Email ID :"+Email_ID);
               System.out.println("Password: "+Password);
               System.out.println("Mail to send message: "+To_Email);

               System.out.println("SendToEmail: "+SendToEmail);
               System.out.println("SendToMobile: "+SendToMobile);
               System.out.println("Send to ES: "+SendToDB);
               System.out.println("Send to MapR: "+SendToMapr);
            }

            Properties props = new Properties();
            props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");


            consumer = new KafkaConsumer<>(props);
            List<String> topics = new ArrayList<String>();
            topics.add(Alerts_Topic);
            consumer.subscribe(topics);
            boolean stop = false;
            int pollTimeout = 55000;

            while (true) {
               //Request unread messages from the topic.
               ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
               Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
               if (iterator.hasNext()) {
                  while (iterator.hasNext()) {

                    //Iterate through returned records, extract the value
                    ConsumerRecord<String, String> record = iterator.next();
                   
                    JSONParser parser = new JSONParser();
                    JSONObject obj = (JSONObject)parser.parse(record.value());

                    if (DEBUG){
                      System.out.println((" Consumed Record: " + record.value()));
                      System.out.println("Weight : "+obj.get("Weight"));
                      System.out.println("Internal_Temperature : "+obj.get("Internal_Temperature"));
                      System.out.println("Weight : "+obj.get("Weight"));
                      System.out.println("Weight : "+obj.get("Weight"));
                    }

                   // Get the fridge ID
                   Integer fridgeID = Integer.parseInt(obj.get("Fridge_id").toString());
                   Double inTemp = Double.parseDouble(obj.get("Internal_Temperature").toString());
                   Double weight = Double.parseDouble(obj.get("Weight").toString());
                   String ts = obj.get("ts").toString();

                   //Create timestamp from ts
                   String timestamp = ts.substring(0,4)+"-"+ts.substring(4,6)+"-"+ts.substring(6,8)+" "+ts.substring(8,10)+":" +ts.substring(10,12)+":"+ts.substring(12,14);
                   //System.out.println(timestamp);
                   //System.out.println(ts);
                   

                   String currentLocation="";
                   if ( obj.get("Coordinates") != null ){
                      currentLocation = obj.get("Coordinates").toString(); 
                   }

                   String lastLocation="";
                   if ( obj.get("Last_Location") != null ){
                      lastLocation = obj.get("Last_Location").toString(); 
                   }

                   Double distance = 0.0;
                   if ( obj.get("Distance") != null ){
                      distance = Double.parseDouble(obj.get("Distance").toString());
                   }

                   String powerStatus = "On";
                   if (obj.get("PowerStatus") != null){
                      powerStatus = obj.get("PowerStatus").toString(); 
                   }

                   //GEnerate the message based ranges
                   String alertMessage = "ข้อความแจ้งเตือนจากบริษัท CPF เรื่องตู้เย็นของท่าน   \nวันและเวลา: "+timestamp+"\nหมายเลขตู้เย็น  "+fridgeID+" \n\n";
                   
                   //Create a shorter message for SMS
                   String phoneMessage = "ขณะนี้ตู้เย็นหมายเลข "+fridgeID+" ของท่าน ";
                    
                   String temperatureStatus = "OK";
                   if (obj.get("TemperatureStatus") != null){
                      temperatureStatus = obj.get("TemperatureStatus").toString(); 
                   }
                   // Get the internal temperature and check for the range
                   if ( temperatureStatus.equalsIgnoreCase("Critical")){
                      //Generate a message
                      alertMessage += "[คำเตือนเกี่ยวกับอุณหภูมิภายในตู้]\n อุณหภูมิภายในที่วัดได้ในขณะนี้ (องศาเซลเซียส)  "+inTemp+".(สูงเกินไป)\n\n";
                      phoneMessage += " อุณหภูมิสูงเกินไป  "+inTemp+" องศาเซลเซียส";
                   }

                   /*String weightStatus = "OK";
                   if (obj.get("WeightStatus") != null){
                      weightStatus = obj.get("WeightStatus").toString(); 
                   }
                   if (weightStatus.equalsIgnoreCase("Critical")){
                      alertMessage += "[Weight Alert]\n Current load in fridge(Kgs): "+weight+".\n\n"; 
                      phoneMessage += " Low weight recorded: "+weight+".";
                   } */
 
                   String locationStatus = "OK";
                   if (obj.get("LocationStatus") != null){
                      locationStatus = obj.get("LocationStatus").toString(); 
                   }
                   if (locationStatus.equalsIgnoreCase("Critical")){
                      alertMessage += "[คำเตือนเรื่องสถานที่ตั้ง]\n ตู้เย็นมีการเคลื่อนย้ายออกจากจุดเดิมห่างออกไปประมาณ  "+distance+" เมตร. \n พิกัดปัจจุบัน   "+currentLocation+"\n พิกัดบันทึกได้ครั้งก่อน  "+lastLocation+"\n\n"; 
                      phoneMessage += " มีการเคลื่อนย้ายไปจากจุดเดิมประมาณ  "+distance+" เมตร.";
                   }

                   if (powerStatus.equalsIgnoreCase("off")){
                      alertMessage += "[คำเตือนเรื่องไฟฟ้าป้อนเข้าเครื่อง] \n   ตู้เย็นไฟดับหรือขาดการติดต่อ. ";
                      phoneMessage += " ตู้เย็นไฟดับหรือขาดการติดต่อ.";
                   }
          
                    System.out.println("===Email AlertMessage===");
                    System.out.println(alertMessage);
                    System.out.println("===SMS Message ===");
                    System.out.println(phoneMessage);
                   
                    String alertRecipients="";
                    //Send Message to registered mobile number
                    if (SendToMobile.equalsIgnoreCase("Yes")){
                    //if (SendToMobile.equalsIgnoreCase("No")){
                       SmsSender.sendSMS(ACCOUNT_SID,AUTH_TOKEN,To_Phone,Twilio_Phone,phoneMessage);
                       alertRecipients += "Phone: "+To_Phone;
                    }

                                   
                    //Send email to registered email ID
                    if (SendToEmail.equalsIgnoreCase("Yes")){
                    //if (SendToEmail.equalsIgnoreCase("No")){
                       EmailSender.sendEmail(Email_ID,Password,alertMessage,To_Email);
                       if (alertRecipients.length() == 0)
                         alertRecipients += "Email: "+To_Email;
                       else
                         alertRecipients += ", Email: "+To_Email;
                    }
                   
                    //Add the additional values to the jsonObject
                    //Email, to_email, phone, phoneMEssage, emailMessage
                    obj.put("Alert Message",phoneMessage); 
                    obj.put("Alert Message Detailed",alertMessage); 

                    if (alertRecipients.length() == 0)
                       alertRecipients = "None";

                    obj.put("Alerts Recipients",alertRecipients); 

                    if (DEBUG)
                       System.out.println("Message for ES : "+obj.toString());
                    
                    if (SendToDB.equalsIgnoreCase("Yes"))
                       ESConnector.sendData(Alerts_Index,obj.toString());

                    //Send to MapR FS
                    if (SendToMapr.equalsIgnoreCase("Yes")){
                       //filewriter = new PrintWriter(new File(maprFile));
                       filewriter.write(obj.toString()+"\n");
                       filewriter.flush();
                    }


            
                  }
               }
            }

        } 
        catch(Exception e){
            e.printStackTrace();
        } finally {
            if (input != null){
               try{
                  input.close();
               }catch(IOException e){
                   e.printStackTrace();
               }
            }
        }
    }
}
