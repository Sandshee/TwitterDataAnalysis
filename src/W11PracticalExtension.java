import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;
import javafx.scene.text.Text;
import javafx.stage.Stage;

/**
 * The main body of code, simply using JavaFX, I felt this was more sensible as a main method,
 * which then called MapperChoices
 */

public class W11PracticalExtension extends Application {
    public static String[] outputStrings = new String[2];

    public static void main(String[] args) {
        launch(args);
    }

    /**
     * The JavaFX method, called by launch.
     *
     * @param primaryStage The stage created by JavaFX.
     */

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("W11 Practical Extension");

        GridPane grid = new GridPane();
        grid.setHgap(10);
        grid.setVgap(10);
        grid.setPadding(new Insets(25, 25, 25, 25));

        Text title = new Text("Please enter input/output paths, and select your choice.");
        grid.add(title, 0, 0, 2, 1);

        Label inputLabel = new Label("Please enter the input path:");
        grid.add(inputLabel, 0, 1);
        TextField inputField = new TextField();
        grid.add(inputField, 1, 1);

        Label outputLabel = new Label("Please enter the output path:");
        grid.add(outputLabel, 0, 2);

        TextField outputField = new TextField();
        grid.add(outputField, 1, 2);

        Label defaultLabel = new Label("Find all most frequent hashtags:");
        Button defaultChoice = new Button();
        defaultChoice.setText("Confirm");
        defaultChoice.setOnAction(new EventHandler<ActionEvent>() {

            /**
             * The user selects to find the most frequent hashtags, so runs MapperChoices, with the input, output and the choice.
             * @param event the actual event that caused this
             */

            @Override
            public void handle(ActionEvent event) {
                if (validInputs(inputField, outputField)) {
                    MapperChoices.mapping(outputStrings, 0);
                }
            }
        });
        grid.add(defaultLabel, 0, 3);
        grid.add(defaultChoice, 1, 3);

        Label frequentTweetLabel = new Label("Find the most frequently retweeted tweets:");
        Button frequentTweets = new Button();
        frequentTweets.setText("Confirm");
        frequentTweets.setOnAction(new EventHandler<ActionEvent>() {

            /**
             * The user selects to find the most popular tweets, so runs MapperChoices, with the input, output and the choice.
             * @param event the actual event that caused this
             */

            @Override
            public void handle(ActionEvent event) {
                if (validInputs(inputField, outputField)) {
                    MapperChoices.mapping(outputStrings, 1);
                }
            }
        });
        grid.add(frequentTweetLabel, 0, 4);
        grid.add(frequentTweets, 1, 4);

        Label frequentUsersLabel = new Label("Find the most frequently retweeted users:");
        Button frequentUsers = new Button();
        frequentUsers.setText("Confirm");
        frequentUsers.setOnAction(new EventHandler<ActionEvent>() {

            /**
             * The user selects to find the most popular users, so runs MapperChoices, with the input, output and the choice.
             * @param event the actual event that caused this
             */

            @Override
            public void handle(ActionEvent event) {
                if (validInputs(inputField, outputField)) {
                    MapperChoices.mapping(outputStrings, 2);
                }
            }
        });
        grid.add(frequentUsersLabel, 0, 5);
        grid.add(frequentUsers, 1, 5);

        Scene scene = new Scene(grid, 600, 300);
        primaryStage.setScene(scene);
        primaryStage.show();
    }

    /**
     * A simple method to check that inputs are valid, then assign them to the outputStrings
     *
     * @param inputField  The input directory supplied
     * @param outputField The output directory supplied
     * @return returns true if successful, false if not, and prints out why not.
     */

    private static Boolean validInputs(TextField inputField, TextField outputField) {
        if (UserInput.validateFile(inputField.getCharacters().toString())) {
            outputStrings[0] = inputField.getCharacters().toString();
            outputStrings[1] = outputField.getCharacters().toString();
            return true;
        }
        if (UserInput.validateFile(inputField.getCharacters().toString()))
            System.out.println("The input file is valid");
        if (UserInput.validateDirectory(outputField.getCharacters().toString()))
            System.out.println("The output file is valid");
        return false;
    }
}
