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
public class DataCompareValue implements Comparator {

    private int order;

    public DataCompareValue() {
        this.order = 1;
    }
    public DataCompareValue(int order) {
        this.order = order;
    }

    @Override
    public int compare(Object o1, Object o2) {
        JEVisSample d1 = (JEVisSample) o1;
        JEVisSample d2 = (JEVisSample) o2;

        int flag = 0;
        try {
            if (d1.getValueAsDouble() - d2.getValueAsDouble() < 0) {
                flag = -1 * order;
            } else if (d1.getValueAsDouble() - d2.getValueAsDouble() > 0) {
                flag = 1 * order;
            } else {
                flag = 0;
            }
        } catch (JEVisException ex) {
            Logger.getLogger(DataCompareValue.class.getName()).log(Level.SEVERE, null, ex);
        }

        return flag;
    }
}
