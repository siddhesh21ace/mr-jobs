package edu.neu.ccs;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @web http://java-buddy.blogspot.com
 */
public class Histogram extends Application {
    long group[] = new long[5];

    @Override
    public void start(Stage primaryStage) {
        groupData();

        Label labelInfo = new Label();

        final CategoryAxis xAxis = new CategoryAxis();
        final NumberAxis yAxis = new NumberAxis();
        final BarChart<String, Number> barChart =
                new BarChart<>(xAxis, yAxis);
        barChart.setCategoryGap(0);
        barChart.setBarGap(1);

        xAxis.setLabel("Number of followers");
        yAxis.setLabel("Number of users");

        XYChart.Series series1 = new XYChart.Series();
        series1.setName("Twitter Followers");
//        series1.getData().add(new XYChart.Data("0-5", Math.log(group[0])));
//        series1.getData().add(new XYChart.Data("5-50", Math.log(group[1])));
//        series1.getData().add(new XYChart.Data("50-5000", Math.log(group[2])));
//        series1.getData().add(new XYChart.Data("5000-50000", Math.log(group[3])));
//        series1.getData().add(new XYChart.Data("50000-I", Math.log(group[4])));

//
        series1.getData().add(new XYChart.Data("1", group[0]));
        series1.getData().add(new XYChart.Data("2-5", group[1]));
        series1.getData().add(new XYChart.Data("6-25", group[2]));
        series1.getData().add(new XYChart.Data("26-I", group[3]));

        barChart.getData().addAll(series1);

        VBox vBox = new VBox();
        vBox.getChildren().addAll(labelInfo, barChart);

        StackPane root = new StackPane();
        root.getChildren().add(vBox);

        Scene scene = new Scene(root, 800, 450);

        primaryStage.setTitle("Twitter Followers Distribution");
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }

    //count data population in groups
    private void groupData() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("input/grouped"));
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s");
                long followerCount = Long.parseLong(words[0]);
                long userCount = Long.parseLong(words[1]);

//                if (userCount > 10000)
//                    continue;

                if (followerCount <= 1) {
                    group[0] += userCount;
                } else if (followerCount <= 5) {
                    group[1] += userCount;
                } else if (followerCount <= 25) {
                    group[2] += userCount;
                } else {
                    group[3] += userCount;
                }
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}