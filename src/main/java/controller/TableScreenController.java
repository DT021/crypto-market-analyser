package controller;

import bean.CurrencyPair;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import spark.BasicSpark;

import java.net.URL;
import java.util.ResourceBundle;

public class TableScreenController implements Initializable {

    @FXML TableView dataTable;

    @FXML ChoiceBox timeRange;

    @FXML TableColumn average;
    @FXML TableColumn min;
    @FXML TableColumn max;
    @FXML TableColumn change;
    @FXML TableColumn volume;
    @FXML TableColumn standardDeviation;

    @FXML Button calculateButton;

    private BasicSpark basicSpark;
    private ObservableList<CurrencyPair> data;

    private String dataFilePath;

    private boolean isDailyChoosen;

    @Override
    public void initialize(URL location, ResourceBundle resources) {
        timeRange.setItems(FXCollections.observableArrayList("Daily", "Monthly"));
        basicSpark = new BasicSpark();

        timeRange.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                if(timeRange.getSelectionModel().getSelectedIndex() == 0){
                    isDailyChoosen = true;
                }else {
                    isDailyChoosen = false;
                }
            }
        });

        calculateButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                try {
                    data = FXCollections.observableArrayList(basicSpark.getWholeData(isDailyChoosen, dataFilePath));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

    }

    public ObservableList<CurrencyPair> getData() {
        return data;
    }

    public void setData(ObservableList<CurrencyPair> data) {
        this.data = data;
    }

    public boolean isDailyChoosen() {
        return isDailyChoosen;
    }

    public void setDailyChoosen(boolean dailyChoosen) {
        isDailyChoosen = dailyChoosen;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public void setDataFilePath(String dataFilePath) {
        this.dataFilePath = dataFilePath;
    }

    @Override
    public String toString() {
        return "TableScreenController{" +
                "dataTable=" + dataTable +
                ", timeRange=" + timeRange +
                ", average=" + average +
                ", min=" + min +
                ", max=" + max +
                ", change=" + change +
                ", volume=" + volume +
                ", standardDeviation=" + standardDeviation +
                ", basicSpark=" + basicSpark +
                ", data=" + data +
                ", dataFilePath='" + dataFilePath + '\'' +
                ", isDailyChoosen=" + isDailyChoosen +
                '}';
    }
}
