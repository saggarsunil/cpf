import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class ESConnector {

        // http://localhost:8080/RESTfulExample/json/product/post
        public static void sendData(String index, String message) {

          try {

                String connectionString = "http://10.85.238.34:9200/"+index+"/alerts";

                URL url = new URL(connectionString);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");

                //String input = "{ \"Timestamp\": \"2017-02-24T19:33:11.021Z\", \"Internal_Temperature\":-1, \"load\":20, \"fridge_id\":21, \"Phone_Number\":\"9845900126\", \"Email\":\"sunil.saggar@gmail.com\", \"Message\":\"CPFoods Alert\" }";

                OutputStream os = conn.getOutputStream();
                os.write(message.getBytes());
                os.flush();

                if (conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
                        throw new RuntimeException("Failed : HTTP error code : "
                                + conn.getResponseCode());
                }

                BufferedReader br = new BufferedReader(new InputStreamReader(
                                (conn.getInputStream())));

                String output;
                System.out.println("Output from Server .... \n");
                while ((output = br.readLine()) != null) {
                        System.out.println(output);
                }

                conn.disconnect();

          } catch (MalformedURLException e) {
                e.printStackTrace();

          } catch (IOException e) {

                e.printStackTrace();

         }

        }

}
  
