package controller;

import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.net.URL;
import java.util.ResourceBundle;

public class MainController implements Initializable {

    @FXML Button dataFileButton;
    @FXML Button analyseButton;
    @FXML Label dataFileNameLabel;
    @FXML Label errorMessageLabel;

    private File dataFile;
    private boolean isDataFileChoosen;

    @Override
    public void initialize(URL location, ResourceBundle resources) {

        isDataFileChoosen = false;

        dataFileButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                FileChooser fileChooser = new FileChooser();
                FileChooser.ExtensionFilter extFilter = new FileChooser.ExtensionFilter("CSV files (*.csv)", "*.csv");
                fileChooser.getExtensionFilters().add(extFilter);
                dataFile = fileChooser.showOpenDialog(dataFileButton.getScene().getWindow());

                dataFileNameLabel.setText(dataFile.getName());
                isDataFileChoosen = true;
                errorMessageLabel.setText("");
            }
        });

        analyseButton.setOnAction(new EventHandler<ActionEvent>() {
            @Override
            public void handle(ActionEvent event) {
                if(isDataFileChoosen){
                    Stage closeStage = (Stage) dataFileNameLabel.getScene().getWindow();
                    closeStage.close();


                    try {

                        FXMLLoader fxmlLoader = new FXMLLoader(new URL("file://" + System.getProperty("user.dir") + "/src/main/java/ui/TableScreen.fxml"));

                        fxmlLoader.load();
                        TableScreenController controller = fxmlLoader.<TableScreenController>getController();
                        controller.setDataFilePath(dataFile.getPath());

                        Parent root = fxmlLoader.getRoot();
                        Scene scene = new Scene(root);

                        Stage dataTableStage = new Stage();
                        dataTableStage.setTitle("Crypto Analyser");
                        dataTableStage.setScene(scene);
                        dataTableStage.setWidth(1320);
                        dataTableStage.setHeight(740);
                        dataTableStage.show();

                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }else {
                    errorMessageLabel.setText("Please choose data file first ..!");
                }
            }
        });
    }
}
