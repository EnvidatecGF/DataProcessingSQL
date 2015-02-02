package org.jevis.jecommons.dataprocessing.sql;

import java.util.List;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisSample;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void output(List<JEVisSample> list) throws JEVisException{
        for(JEVisSample sample:list){
            System.out.println(sample.getTimestamp()+";"+sample.getValue());
        }
    }

}
