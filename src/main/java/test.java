import java.net.*;
import java.io.*;

public class test {
    static String userInput;

    public static void main(String [] args) throws IOException {
        InetAddress serverAddress = InetAddress.getByName("localhost");
        try (
                Socket socket = new Socket(serverAddress, 9999);
                BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        ) {
            while ((userInput = input.readLine()) != null) {
                System.out.println(input.readLine());
            }
        }
    }
}
