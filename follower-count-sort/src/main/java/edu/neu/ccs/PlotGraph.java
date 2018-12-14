package edu.neu.ccs;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.ScatterChart;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import java.io.BufferedReader;
import java.io.FileReader;

public class PlotGraph extends Application {

    @Override
    public void start(Stage stage) {
        stage.setTitle("Twitter Followers");
        final NumberAxis xAxis = new NumberAxis();
        final NumberAxis yAxis = new NumberAxis();
        final ScatterChart<Number, Number> sc = new ScatterChart<>(xAxis, yAxis);
        xAxis.setLabel("Number of followers");
        yAxis.setLabel("Number of users");

        XYChart.Series series1 = new XYChart.Series();
        series1.setName("Twitter Followers");

        try {
            BufferedReader br = new BufferedReader(new FileReader("input/grouped"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s");

                long followerCount = Long.parseLong(words[0]);
                long userCount = Long.parseLong(words[1]);

                if (followerCount > 10)
                    continue;
                series1.getData().add(new XYChart.Data(followerCount, userCount));
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        sc.getData().addAll(series1);
        Scene scene = new Scene(sc);
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}