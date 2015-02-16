/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.jecommons.dataprocessing;

import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisSample;

/**
 *
 * @author gf
 */

public class DataCompareTime implements Comparator {
    private int order;
    
    public DataCompareTime(){
        order=1;
    }
    public DataCompareTime(int order){
        this.order=order;
    }
    
    @Override
    public int compare(Object arg0, Object arg1) {
        JEVisSample d1 = (JEVisSample) arg0;
        JEVisSample d2= (JEVisSample) arg1;

        int flag = 0;
        try {
            flag = d1.getTimestamp().compareTo(d2.getTimestamp())*order;
        } catch (JEVisException ex) {
            Logger.getLogger(DataCompareTime.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return flag;
}
    
}
