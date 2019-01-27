import java.util.logging.Logger;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
/**
 * className: LoggerGenerator
 * description: TODO
 *
 * @author hasee
 * @version 1.0
 * @date 2019/1/25 23:53
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());
    public static void main(String[] args) throws InterruptedException {
        BasicConfigurator.configure ();
        PropertyConfigurator.configure("src/test/resources/log4j.properties");
        int index = 0;
        while (true) {
            Thread.sleep(1000);
            logger.info("values :" + index++);
        }
    }
}
