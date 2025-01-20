package wc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Graph {
    public static void main(String[] args) {
        try {
            FileWriter writer = new FileWriter("input/graph.txt", true);


            writer.write("0 0 0");
            writer.write("\r\n");   // write new line
            for (int i = 1; i <= 3 * 3; i++) {

                if (i % 3 == 0) {
                    String self = i +"";
                    String toPage = 0 + "";
                    double pr = 1.0/(3*3);
                    String pageRank = String.valueOf(pr);
                    writer.write(self + " " + toPage + " " + pageRank);
                    writer.write("\r\n");   // write new line

                } else {
                    String self = i +"";
                    String toPage = (i+1) + "";
                    double pr = 1.0/(3*3);
                    String pageRank = String.valueOf(pr);
                    writer.write(self + " " + toPage + " " + pageRank);
                    writer.write("\r\n");   // write new line
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

