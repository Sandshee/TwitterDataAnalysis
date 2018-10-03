import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Intended to just ensure that the user was giving valid variables.
 */

public class UserInput {

    /**
     * A currently redundant method, created before a graphical interface was used.
     *
     * @param message Takes in what to ask the user.
     * @return returns the user's response as an integer only.
     */
    protected static int getChoice(String message) {
        System.out.println(message);
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.next();
            try {
                return Integer.parseInt(input);
            } catch (NumberFormatException e) {
                //Input was not desired format. Bugger you.
                System.out.println("Please enter an integer");
            }
        }
    }

    /**
     * Just ensures that the input file is valid.
     *
     * @param file the single file to be validated.
     * @return true if the file exists, false if not.
     */

    protected static boolean validateFile(String file) {
        File tempFile = new File(file);
        return tempFile.exists();
    }

    /**
     * Ensures that the directory specified is writable if it exists so it can be deleted.
     *
     * @param directory The input directory that is being checked.
     * @return true if the directory is writable, false if not.
     */

    protected static boolean validateDirectory(String directory) {
        File tempDirectory = new File(directory);
        return tempDirectory.canWrite();
    }
}
