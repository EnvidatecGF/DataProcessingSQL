/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.jecommons.dataprocessing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisSample;
import org.jevis.jecommons.dataprocessing.DataCalculation;
import org.joda.time.DateTime;

/**
 *
 * @author gf
 */
public class DataCalc implements DataCalculation{

    @Override
    public List<JEVisSample> addition(List<JEVisSample> samples, double value) throws JEVisException {
        List<JEVisSample> sample_add = new ArrayList<JEVisSample>();

        Locale.setDefault(Locale.US); //avoid the different form of decimal point
        
        BigDecimal num; //creat a "BigDeciaml" variable
        BigDecimal val=new BigDecimal(value); //creat a "BigDeciaml" variable and convert "value" to BigDecimal
        for (JEVisSample row : samples) {
            num=new BigDecimal(row.getValueAsString()); //the convertion is to avoid the number of decimals,because of double

            JEVisSample samp=row.getAttribute().buildSample(row.getTimestamp(), num.add(val).doubleValue());//convert the result and it's time to typ JEVisSample
            sample_add.add(samp);//put the result into a list
        }

        return sample_add;
    }

    @Override
    public List<JEVisSample> addition(List<List<JEVisSample>> attributes) throws JEVisException {
        
        HashMap<DateTime, String> timestamps = new HashMap<DateTime, String>();//timestamps is to add all timestamps of every attribute(data row)
        HashMap<DateTime, Double> result_map = new HashMap<DateTime, Double>();
        
        for(List<JEVisSample> samples:attributes){
            timestamps.putAll(listToMap(samples));//put all timestamps in, as a scalar
        }
        for (Map.Entry entry : timestamps.entrySet()) {
            BigDecimal sum=new BigDecimal(0); 
            BigDecimal value;
            for(List<JEVisSample> samples:attributes){
                HashMap<DateTime, String> map_att = listToMap(samples); //convert every data row to a Map for convenience of seeking
                if(map_att.containsKey(entry.getKey())){ //if every data row contains the timestamps in "timestamps",then calculate
                    value=new BigDecimal(map_att.get(entry.getKey()));
                    sum=sum.add(value);
                }
                result_map.put((DateTime)entry.getKey(), sum.doubleValue());//connect the result to it's time 
            }
        }

        return mapToSortList(result_map, attributes.get(0).get(0).getAttribute());
    }
    
    public List<JEVisSample> addition(List<JEVisSample> samples1, List<JEVisSample> samples2) throws JEVisException {
        HashMap<DateTime, String> map1 = listToMap(samples1); //convert sample1 to a Map
        HashMap<DateTime, String> map2 = listToMap(samples2); //convert sample2 to a Map
        HashMap<DateTime, Double> result_map = new HashMap<DateTime, Double>(); //creat a result_map to put the result in

        BigDecimal sum;
        for (Map.Entry entry : map1.entrySet()) {
            if (map2.get(entry.getKey()) != null) {//If there is a same time points in sample2,add
                sum = new BigDecimal((String)entry.getValue());//first instance for BigDecimal,then add
                sum=sum.add(new BigDecimal(map2.get(entry.getKey())));
            } else {
                sum = new BigDecimal((String)entry.getValue());////If there is no same time points in sample2,direct put into the List
            }
            result_map.put((DateTime) entry.getKey(), sum.doubleValue());
        }

        //to find the values in second data row, that is not be added
        for (Map.Entry entry : map2.entrySet()) {
            if (!result_map.containsKey(entry.getKey())) {
                result_map.put((DateTime) entry.getKey(), new BigDecimal((String)entry.getValue()).doubleValue());
            }
        }

        return mapToSortList(result_map, samples1.get(0).getAttribute());
    }
                 
    /*
    the combination of high pass filter und low pass filter
    The input parameter ?delete¡° decides, whether the improper values 
    will be deleted or replaced by  the upper limit and  the lower limit.
    */
    @Override
    public List<JEVisSample> boundaryFilter(List<JEVisSample> samples, double boundary_up, double boundary_low, boolean delete) throws JEVisException {
        List<JEVisSample> sample_bf = new ArrayList<JEVisSample>();//creat a List to put the result
        JEVisSample newsample;

        if(delete){//delete=true,don't store the ineligible value and its'time point
            for (JEVisSample circ : samples) {
            if (circ.getValueAsDouble() <= boundary_up && circ.getValueAsDouble() >= boundary_low) {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), circ.getValueAsDouble());
                sample_bf.add(newsample);
            }
        }
        }else{//delete=false,replace the ineligible value with upper limit and low limit
            for (JEVisSample circ : samples) {
            if (circ.getValueAsDouble() > boundary_up) {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), boundary_up);
            } else if (circ.getValueAsDouble() < boundary_low) {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), boundary_low);
            } else {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), circ.getValueAsDouble());
            }
            sample_bf.add(newsample);
        }
        }
        return sample_bf;
    }

    /*
    
    */
    @Override
    public List<JEVisSample> cumulativeDifferentialConverter(List<JEVisSample> samples) throws JEVisException {
        List<JEVisSample> sample_cd = new ArrayList<JEVisSample>();//creat a List to put the result
        DateTime time;
        BigDecimal value = null;
        int count = 0;

        for (JEVisSample row : samples) {
            count++;//to judge the first data

            if (count == 1) {
                value = new BigDecimal(row.getValueAsString());//convert the first value to BigDecimal for the preparation of calculation
                sample_cd.add(row.getAttribute().buildSample(row.getTimestamp(), row.getValueAsDouble()));//directly put the first data into the result List
            } else {
                value = new BigDecimal(row.getValueAsString()).subtract(value);//the last value subtracts the previous one
                time = row.getTimestamp();
                sample_cd.add(row.getAttribute().buildSample(time, value.doubleValue()));
                value = new BigDecimal(row.getValueAsString());//change the next previous data
            }
        }
        return sample_cd;
    }

    /*
     * 1. the over boundary value will not be stored, and become a gap.  no
     * 2. the over boundary value will replaced by boundary.
     * 3. the over boundary value will replaced by 0 or any other value.
     * here choose 2
     */
    @Override
    public List<JEVisSample> highPassFilter(List<JEVisSample> samples, double boundary) throws JEVisException {
        List<JEVisSample> sample_hpf = new ArrayList<JEVisSample>();//creat a List to put the result
        JEVisSample newsample;//to get the eligible values(JEVisSample)

        for (JEVisSample circ : samples) {
            if (circ.getValueAsDouble() > boundary) {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), circ.getValueAsDouble());
            } else {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), boundary);
            }
            sample_hpf.add(newsample);
        }
        return sample_hpf;
    }

    /*
     * 1. the over boundary value will not be stored, and become a gap.  no
     * 2. the over boundary value will replaced by boundary.
     * 3. the over boundary value will replaced by 0 or any other value.
     * here choose 3
     */
    @Override
    public List<JEVisSample> highPassFilter(List<JEVisSample> samples, double boundary, double fill_value) throws JEVisException {
        List<JEVisSample> sample_hpf = new ArrayList<JEVisSample>();//creat a List to put the result
        JEVisSample newsample;//to get the eligible values(JEVisSample)


        for (JEVisSample circ : samples) {
//            list.setTime(circ.getTimestamp());
            if (circ.getValueAsDouble() > boundary) {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), circ.getValueAsDouble());
            } else {
                newsample = circ.getAttribute().buildSample(circ.getTimestamp(), fill_value);
            }
            sample_hpf.add(newsample);
        }
        return sample_hpf;
    }

    /*
     * calculate the area between every two time points,and here the period
     * between every two time points must not be same.
     * And there could be sevrial methods to calculate the integration:vorwaerts,nachwaerts,trapezoid, 
     * here trapezoid is used.
     */
    @Override
    public double integration(List<JEVisSample> samples) throws JEVisException {
        BigDecimal integration =new BigDecimal("0");
        BigDecimal[] time_interval = new BigDecimal[samples.size() - 1];  //the interval between every two time points

        for (int i = 0; i < samples.size() - 1; i++) {
            //the unit of time_interval is second
            time_interval[i] = new BigDecimal(samples.get(i + 1).getTimestamp().getSecondOfDay()).subtract(new BigDecimal(samples.get(i).getTimestamp().getSecondOfDay()));
            if (time_interval[i].equals(new BigDecimal("0"))) {
                throw new IllegalArgumentException("X must bemontotonic. A duplicate " + "x-value was found");
            }
        }

        for (int i = 0; i < samples.size() - 1; i++) {
            BigDecimal num1=new BigDecimal(samples.get(i).getValueAsString());
            BigDecimal num2=new BigDecimal(samples.get(i + 1).getValueAsString());
            integration= integration.add(num1.add(num2).multiply(new BigDecimal("0.5")).multiply(time_interval[i]));
            //integration += 0.5 * (num1 + num2)* dx[i];
        }
        return integration.doubleValue();
    }
    
    /*
     * calculate the area between every two time points,and here the period
     * between every two time points must not be same.
     * And there could be sevrial methods to calculate the integration:vorwaerts,nachwaerts,trapezoid, 
     * here trapezoid is used.
     * calculate in a range
     */
    public double integration(JEVisAttribute sample, DateTime from, DateTime to) throws JEVisException {
        BigDecimal integration =new BigDecimal("0");
        BigDecimal[] time_interval = new BigDecimal[sample.getAllSamples().size() - 1];  //the interval between every two time points
        
        List<JEVisSample> cuted =sample.getSamples(from, to);
        
        DataCompareTime comparator = new DataCompareTime();
        Collections.sort(cuted, comparator);
        
        for (int i = 0; i < cuted.size() - 1; i++) {
            time_interval[i] = new BigDecimal(cuted.get(i + 1).getTimestamp().getSecondOfDay()).subtract(new BigDecimal(cuted.get(i).getTimestamp().getSecondOfDay()));
            if (time_interval[i].equals(new BigDecimal("0"))) {
                throw new IllegalArgumentException("X must bemontotonic. A duplicate " + "x-value was found");
            }
        }

        for (int i = 0; i < cuted.size() - 1; i++) {
            BigDecimal num1=new BigDecimal(cuted.get(i).getValueAsString());
            BigDecimal num2=new BigDecimal(cuted.get(i + 1).getValueAsString());
            integration= integration.add(num1.add(num2).multiply(new BigDecimal("0.5")).multiply(time_interval[i]));
            //integration += 0.5 * ( +  )* dx[i];
        }

        return integration.doubleValue();
    }

    /*
     * eliminate the deviation of the timestamp(delay). 
     * the smallest unit of time,period_s and deviation_s is second
     */
    @Override
    public List<JEVisSample> intervalAlignment(List<JEVisSample> samples, DateTime begin_time, int period_s, int deviation_s) throws JEVisException {//, DateTime begin_time
        List<JEVisSample> sample_ia = new ArrayList<JEVisSample>();//creat a List to put the result
        List<DateTime> time_scalar= new ArrayList<DateTime>();
        DateTime right_time=begin_time;
                    
        List<JEVisSample> sample_sort=sortByTime(samples,1);
        
        while (right_time.plusSeconds(period_s).isBefore(sample_sort.get(sample_sort.size() - 1).getTimestamp())) {
            time_scalar.add(right_time);
            right_time = right_time.plusSeconds(period_s);
        }
        time_scalar.add(right_time);
        int time_before=time_scalar.get(0).getSecondOfDay()-deviation_s;

        for (JEVisSample row : sample_sort) {

            for (int i = 0; i <= time_scalar.size() - 1; i++) {
                boolean before = row.getTimestamp().isBefore(time_scalar.get(i).plusSeconds(deviation_s));
                boolean after = row.getTimestamp().isAfter(time_scalar.get(i).minusSeconds(deviation_s));
                if (after && before) {
                    JEVisSample cash = row.getAttribute().buildSample(time_scalar.get(i), row.getValue());
                    if (listToMap(sample_ia).containsKey(cash.getTimestamp())) {
                        if (Math.abs(time_before - time_scalar.get(i).getSecondOfDay()) > Math.abs(row.getTimestamp().getSecondOfDay() - time_scalar.get(i).getSecondOfDay())) {
                            sample_ia.set(sample_ia.size() - 1, cash);
                        }
                    } else {
                        sample_ia.add(cash);
                    }
                    break;
                }
            }
            time_before = row.getTimestamp().getSecondOfDay();
        }
        
//        for (JEVisSample row : samples) {
//            boolean before = row.getTimestamp().isBefore(right_time.plusSeconds(deviation_s));
//            boolean after = row.getTimestamp().isAfter(right_time.minusSeconds(deviation_s));
//            if (after && before) {
//                sample_ia.add(row.getAttribute().buildSample(right_time, row.getValue()));
//            }
//            right_time = right_time.plusSeconds(period_s);
//        }
        
//        for(JEVisSample row : sample.getAllSamples()){
//            int remainder=row.getTimestamp().getSecondOfDay()% period_s;//get the remainder of the sampled time/period
//            if(remainder <= period_s && remainder >= (period_s-deviation_s)){//the sampled time is earlier than the right time or equals to the right time
//                right_time=row.getTimestamp().plusSeconds(period_s-remainder);
//                sample_ia.add(sample.buildSample(right_time, row.getValue()));
//            }else if(remainder >= 0 && remainder <= deviation_s){//the sampled is later than the right time or the right time
//                right_time=row.getTimestamp().minusSeconds(remainder);
//                sample_ia.add(sample.buildSample(right_time, row.getValue()));
//            }
//        }

        return sample_ia;
    }

    /*
     * interpolation the whole data row, from begin to end
     */
    @Override
    public List<JEVisSample> linearInterpolation(List<JEVisSample> samples, int insert_num) throws JEVisException {
        List<JEVisSample> sample_i = new ArrayList<JEVisSample>();//creat a List to put the result
        double value;
        DateTime time;

        double[] dx = new double[samples.size() - 1];//to get the differences between every two adjacent times
        double[] dy = new double[samples.size() - 1];//to get the differences between every two adjacent vaules

        Locale.setDefault(Locale.US);
        
        //calculate the differences
        for (int i = 0; i < samples.size() - 1; i++) {
            dx[i] = samples.get(i + 1).getTimestamp().getSecondOfDay() - samples.get(i).getTimestamp().getSecondOfDay();
            if (dx[i] == 0) {
                throw new IllegalArgumentException("X must bemontotonic. A duplicate " + "x-value was found");
            }
            dy[i] = samples.get(i + 1).getValueAsDouble()-samples.get(i).getValueAsDouble();
        }

        for (int i = 0; i < samples.size()-1; i++) {
            for (int j = 0; j < insert_num + 1; j++) {
                int nb = insert_num + 1;
                JEVisSample data;
                time = samples.get(i).getTimestamp().plusSeconds((int) (dx[i] * j / nb));
                value = samples.get(i).getValueAsDouble() + dy[i]*j/nb;
                DecimalFormat format=new DecimalFormat("0.#####");
                data = samples.get(0).getAttribute().buildSample(time, Double.parseDouble(format.format(value)));
                sample_i.add(data);
            }
        }
        sample_i.add(samples.get(0).getAttribute().buildSample(samples.get(samples.size()-1).getTimestamp(), samples.get(samples.size()-1).getValue()));
        return sample_i;
    }
    
        /*
     * interpolation in a range
     */
    public List<JEVisSample> linearInterpolation(JEVisAttribute sample,DateTime from, DateTime to, int insert_num) throws JEVisException {
        List<JEVisSample> sample_i = new ArrayList<JEVisSample>();//creat a List to put the result
        double value;
        DateTime time;

        List<JEVisSample> cuted = sample.getSamples(from, to);//new ArrayList<JEVisSample>();
        List<JEVisSample> rest1 = sample.getSamples(sample.getAllSamples().get(0).getTimestamp(), from);//cut the original elements,that don't need to be changed
        rest1.remove(rest1.size()-1);//remove from
        List<JEVisSample> rest2 = sample.getSamples(to, sample.getLatestSample().getTimestamp());//cut the original elements,that don't need to be changed
        rest2.remove(0);//remove to
        
        double[] dx = new double[cuted.size() - 1];
        double[] dy = new double[cuted.size() - 1];

        Locale.setDefault(Locale.US);
        
        for (int i = 0; i < cuted.size() - 1; i++) {
            dx[i] = cuted.get(i + 1).getTimestamp().getSecondOfDay() - cuted.get(i).getTimestamp().getSecondOfDay();
            if (dx[i] == 0) {
                throw new IllegalArgumentException("X must bemontotonic. A duplicate " + "x-value was found");
            }
            dy[i] = cuted.get(i + 1).getValueAsDouble()-cuted.get(i).getValueAsDouble();
        }

        for (int i = 0; i < cuted.size()-1; i++) {
            for (int j = 0; j < insert_num + 1; j++) {
                int nb = insert_num + 1;
                time = cuted.get(i).getTimestamp().plusSeconds((int) (dx[i] * j / nb));
                value = cuted.get(i).getValueAsDouble() + dy[i] * j / nb;
                DecimalFormat format=new DecimalFormat("0.#####");
                sample_i.add(sample.buildSample(time, Double.parseDouble(format.format(value))));
            }
        }
        sample_i.add(sample.buildSample(cuted.get(cuted.size()-1).getTimestamp(), cuted.get(cuted.size()-1).getValue()));
        sample_i.addAll(rest1);//add the original elements,that don't need to be changed
        sample_i.addAll(rest2);//add the original elements,that don't need to be changed
        
        DataCompareTime comparator = new DataCompareTime();
        Collections.sort(sample_i, comparator);
        
        return sample_i;
    }

    /*
     * calculate every value according to y=a*x+b
     * the gap is not taken into considered until now
     */
    @Override
    public List<JEVisSample> linearScaling(List<JEVisSample> samples, double proportion, double b) throws JEVisException {
        List<JEVisSample> sample_ls = new ArrayList<JEVisSample>();//creat a List to put the result
        BigDecimal p=new BigDecimal(proportion);//convert to the BigDecimal to avoid the number of decimals,because of double
        BigDecimal bb=new BigDecimal(b);//convert to the BigDecimal to avoid the avoid the number of decimals,because of double

        for (JEVisSample circ : samples) {
            sample_ls.add(circ.getAttribute().buildSample(circ.getTimestamp(), new BigDecimal(circ.getValueAsString()).multiply(p).add(bb).doubleValue()));//calculation

        }
        return sample_ls;
    }
    
    /*
    look for the median of one data row(JEVis variable). 
    */
    @Override
    public double median(List<JEVisSample> samples) throws JEVisException {
        Double median;
        List<JEVisSample> list_med = new ArrayList<JEVisSample>();//get all daten,are not sorted according to value yet
        list_med.addAll(samples);
        
        DataCompareValue comparator = new DataCompareValue();
        Collections.sort(list_med, comparator);//be sorted according to value, from small to big

//        for(JEVisSample s:list_med){
//            System.out.println(s.getTimestamp()+";"+s.getValue());
//        }

        if (list_med.size() % 2 != 0) {
            median = list_med.get((list_med.size() + 1) / 2).getValueAsDouble();//odd
        } else {
//            System.out.println(list_med.size() / 2);
//            System.out.println(list_med.get(list_med.size() / 2).getValueAsDouble());
//            System.out.println(list_med.size() / 2 + 1);
//            System.out.println(list_med.get(list_med.size() / 2 - 1).getValueAsDouble());
            BigDecimal num1=new BigDecimal(list_med.get(list_med.size() / 2).getValueAsString());
            BigDecimal num2=new BigDecimal(list_med.get(list_med.size() / 2 - 1).getValueAsString());
            median = num1.add(num2).divide(new BigDecimal("2")).doubleValue(); //even
        }
        return median;
    }

    /*
     * merge the values at some timestamps to one timestamp, the values will be added. 
     * The input parameter ?begin_time¡° is the theoretic begin time(the first sampled time) of the data row. 
     * The input parameter ?period_s¡° should be time(period) and it's unit is  second. 
     * The last input parameter ?meg_num¡° means, how many sampled value will be merged.
     * (millinsecond for year is too lang,already beyound the int,so the smallest unit of time is here second.)
     */
    @Override
    public List<JEVisSample> mergeValues(List<JEVisSample> samples, DateTime begin_time, int period_s, int meg_num) throws JEVisException {  //seg_num:the number of merge
        List<JEVisSample> sample_mv = new ArrayList<JEVisSample>();//creat a List to put the result
        BigDecimal sum= new BigDecimal("0");
        DateTime time = begin_time;//get the theoretic begin time
        DateTime time_l = new DateTime();

        for (JEVisSample row : samples) {
            if (row.getTimestamp().isBefore(time.plusSeconds(period_s * meg_num))) {
                sum = sum.add(new BigDecimal(row.getValueAsString()));
                time_l = time.plusSeconds(period_s * (meg_num - 1));//time_l=row.getTime();
            } else {
                sample_mv.add(row.getAttribute().buildSample(time_l, sum.doubleValue()));
                time = time.plusSeconds(period_s * meg_num);
                sum = new BigDecimal("0");
                sum = sum.add(new BigDecimal(row.getValueAsString()));
                time_l = time.plusSeconds(period_s * (meg_num - 1));//time_l=row.getTime();
            }
        }
        sample_mv.add(samples.get(0).getAttribute().buildSample(time_l, sum.doubleValue()));//.doubleValue()
        return sample_mv;
    }

    /*
    delete the value,that is not bigger or smaller than it's previous value in one percentage value. 
    The inputparameter ?percent¡° is the percentage value, which is decided by enduser.
    */
    @Override
    public List<JEVisSample> precisionFilter(List<JEVisSample> samples, double percent) throws JEVisException {
        List<JEVisSample> sample_pf = new ArrayList<JEVisSample>();//create a List to put the result
        List<JEVisSample> sa = new ArrayList<JEVisSample>();//get all daten
        sa.addAll(samples);
        double perc;//get the calculated percent

        JEVisSample s = samples.get(0).getAttribute().buildSample(sa.get(0).getTimestamp(), sa.get(0).getValueAsDouble());//get the first value
        //s kann nicht getValueAsDouble() anrufen    (Double)      
        sample_pf.add(s);

        for (int j = 1; j < sa.size(); j++) {
//            System.out.println(s.getValue());
//            System.out.println(sa.get(j).getValueAsDouble());
            perc = Math.abs(sa.get(j).getValueAsDouble() - (Double) s.getValue()) / (Double) s.getValue();//calculate the percent of every two adjacent value
            if (perc >= percent) {
                s = samples.get(0).getAttribute().buildSample(sa.get(j).getTimestamp(), sa.get(j).getValueAsDouble());//get the next eligible value,to be calculated next time
                sample_pf.add(s);
            }
        }

        return sample_pf;
    }

    /*
    sort the data row according to Time. The import parameter "order" decides form begin/end to end/begin.
    order= 1,begin to end
    order=-1,end to begin
    */
    @Override
    public List<JEVisSample> sortByTime(List<JEVisSample> samples,int order) {
        List<JEVisSample> sample_sbt = new ArrayList<JEVisSample>();
        sample_sbt.addAll(samples);

        DataCompareTime comparator = new DataCompareTime(order);
        Collections.sort(sample_sbt, comparator);

        return sample_sbt;
    }
    
    /*
    sort the data row according to value. The import parameter "order" decides form big/small to small/big.
    order= 1,small to big
    order=-1,big to small
    */
    @Override
    public List<JEVisSample> sortByValue(List<JEVisSample> samples,int order) {
        List<JEVisSample> sample_sbv = new ArrayList<JEVisSample>();
        sample_sbv.addAll(samples);
        
        DataCompareValue comparator = new DataCompareValue(order);
        Collections.sort(sample_sbv, comparator);

        return sample_sbv;
    }
    
    /*
     * only split the value as average, it's not komplete
     */
    @Override
    public List<JEVisSample> splitValues(List<JEVisSample> samples, int period_s, int seg_num,Boolean backward) throws JEVisException {  //seg_num:the number of segmentation
        List<JEVisSample> sample_sv = new ArrayList<JEVisSample>();
        //int count = seg_num;
        BigDecimal value_n = new BigDecimal("0");
        DateTime time;

        if (seg_num >= 2) {
            for (JEVisSample row : samples) {
                for (int i = 1; i <= seg_num; i++) {
                    if(backward==null || backward.equals(false)){
                        time = row.getTimestamp().minusSeconds((seg_num - i) * period_s / (seg_num));
                    }else{
                        time = row.getTimestamp().plusSeconds((i-1) * period_s / (seg_num));
                    }
                    
                    value_n = new BigDecimal(row.getValueAsString()).divide(new BigDecimal(seg_num), 10, RoundingMode.HALF_UP);//if can't be divided with no remainder,then keep 10 decimals
                    sample_sv.add(row.getAttribute().buildSample(time, value_n.doubleValue()));
                }

//                sample_sv.add(samples.get(0).getAttribute().buildSample(row.getTimestamp(), value_n.doubleValue()));
            }
        }else if(seg_num ==1){
            for (JEVisSample row : samples) {
                sample_sv.add(samples.get(0).getAttribute().buildSample(row.getTimestamp(), row.getValueAsDouble()));
            }
        }else{
            System.out.println("The data row can't be split.");
        }

        return sample_sv;
    }
    
    /*
    every value of the data row minus one value
    */
    @Override
    public List<JEVisSample> subtraction(List<JEVisSample> samples, double value) throws JEVisException {
        List<JEVisSample> sample_sub = new ArrayList<JEVisSample>();
        
        Locale.setDefault(Locale.US);//avoid the different form of decimal point
        BigDecimal num;
        BigDecimal v=new BigDecimal(value);//creat a "BigDeciaml" variable and convert "value" to BigDecimal
        for (JEVisSample row : samples) {
            num=new BigDecimal(row.getValueAsString());//the convertion is to avoid the number of decimals,because of double
            JEVisSample samp=row.getAttribute().buildSample(row.getTimestamp(), num.subtract(v).doubleValue());
            sample_sub.add(samp);
        }

        return sample_sub;
    }

    /*
    the value in first data row minus the value in second data row with same time.
    the time punkts,that only the first data row has, it's value will be directly putted into the result.
    the time punkts,that only the second data row has, it's value will become negative and be putted into the result.
    */
    @Override
    public List<JEVisSample> subtraction(List<JEVisSample> samples1, List<JEVisSample> samples2) throws JEVisException {
        HashMap<DateTime, String> map1 = listToMap(samples1);//convert sample1 to a Map
        HashMap<DateTime, String> map2 = listToMap(samples2);//convert sample2 to a Map
        HashMap<DateTime, Double> result_map = new HashMap<DateTime, Double>();

        BigDecimal sub;
        for (Map.Entry entry : map1.entrySet()) {
            if (map2.get(entry.getKey()) != null) {//if both data rows has the same time punkt,the first value minus the second value
                sub = new BigDecimal((String)entry.getValue());//convert to the BigDecimal to avoid the avoid the number of decimals,because of double
                sub=sub.subtract(new BigDecimal(map2.get(entry.getKey())));
            } else {
                sub = new BigDecimal((String)entry.getValue());
            }
            result_map.put((DateTime) entry.getKey(), sub.doubleValue());

        }

        //to find the values in second data row, that is not be substracted
        for (Map.Entry entry : map2.entrySet()) {
            if (!result_map.containsKey(entry.getKey())) {
                result_map.put((DateTime) entry.getKey(), -(Double) entry.getValue());
            }
        }

        return mapToSortList(result_map, samples1.get(0).getAttribute());//convert the map to List,and return
    }

    /*
     * output all minimum values with their time in the data row
     */
    @Override
    public List<JEVisSample> valueAllMinimum(List<JEVisSample> samples) throws JEVisException {
        List<JEVisSample> multiple_min = new ArrayList<JEVisSample>();
        
        //use valueMinimum(JEVisAttribute sample) to search the smallest value in the data roll
        double min=valueMinimum(samples);

        //with the smallest vallue,that is found before, find all smallest values in the data roll
        for (JEVisSample circ : samples) {
            if (min==circ.getValueAsDouble()) {
                multiple_min.add(circ.getAttribute().buildSample(circ.getTimestamp(), circ.getValueAsDouble()));//creat a variable of typ JeVisSample and put it into List
            }
        }
        return multiple_min;
    }

    /*
     * output only the minimum value in one data row
     */
    @Override
    public double valueMinimum(List<JEVisSample> samples) throws JEVisException {
        double min = 0;

        min = samples.get(0).getValueAsDouble();
        //search the smallest value in the data roll
        for (JEVisSample circ1 : samples) {
            if (circ1.getValueAsDouble() < min) {
                    min = circ1.getValueAsDouble();
                }
        }
        return min;
    }

//    public double valueMinimum(List<Double> values){
//        double min_result=values.get(0);
//        for(double value:values){
//            min_result=Math.min(value, min_result);
//        }
//        return min_result;
//    }//geprueft wird
    
    /*
    find the minimum value of multiple data rows and a value. 
    the multiple data rows must first be putted into a List, so that this
    function can compare endless more data rows.
    */
    @Override
    public double valueMinimumMore(List<List<JEVisSample>> attributes) throws JEVisException {

        double min;
        double min_result=valueMinimum(attributes.get(0));//find the minimum value in first data row as the first min_result
        for(List<JEVisSample> samples:attributes){
            min=valueMinimum(samples);//find the minimum value in the rest data rows and compare it to min_result
            if(min<min_result){
                min_result=min;//if find a smaller, assign it to min_result
            }
        }
        return min_result;
    }

    /*
     find the minimum value of multiple data rows and a value. 
     It firstly use the function:valueMinimum(List<JEVisAttribute> attributes) to find 
     the minimum value of multiple data rows, then compare it to the value.
     */
    @Override
    public double valueMinimumMore(List<List<JEVisSample>> attributes, double value) throws JEVisException {
        double min = valueMinimumMore(attributes);//use valueMinimum(List<JEVisAttribute> attributes) to find the minimum value of multiple data rows
        double min_result;

        if (min > value) {
            min_result = value;
        } else {
            min_result = min;
        }
        return min_result;
    }

    /*
     * this function is to find,whether a data row has gaps.
     * if yes, put the begintime and endtime of gaps into a List, every two times is a pair
     */
    public List<DateTime> findGap(List<JEVisSample> samples, DateTime begin_time, int period_s, int deviation_s) throws JEVisException {
//        System.out.println("If the function \"intervalAlignment\" is not used before the function \"findGap\",there will be a wrong result!!");

        List<JEVisSample> list = intervalAlignment(samples,begin_time, period_s, deviation_s);//use "intervalAlignment" to eliminate the deviation of sampled time
        List<DateTime> gap_time = new ArrayList<DateTime>();//create a List to put the gap's time

        for (int i = 0; i < list.size() - 1; i++) {
            int sec1 = list.get(i + 1).getTimestamp().getSecondOfDay();
            int sec2 = list.get(i).getTimestamp().getSecondOfDay();
            //get the seconds of every two times

            if ((sec1 - sec2) > period_s) {//if thedifference of every two times is bigger than the period,put these two time into the List
                gap_time.add(list.get(i).getTimestamp());
                gap_time.add(list.get(i + 1).getTimestamp());
            }
        }

        return gap_time;
    }

    /*
     * this function is to simplify the calculation of other functions. 
     * This function converts  JEVis variable(JEVisAttribute) to the map.
     * one element of the map is "String" not "Double", just for the use of "BigDecimal".
     */
    private static HashMap<DateTime, String> listToMap(List<JEVisSample> samples) throws JEVisException {
        HashMap<DateTime, String> map = new HashMap<DateTime, String>();

        for (JEVisSample d : samples) {
            map.put(d.getTimestamp(), d.getValueAsString());
        }

        return map;
    }

    /*
     * this function is to simplify the calculation of other functions. 
     * This function converts the map to JEVis variable(JEVisSample)
     * and sort the daten according to time. 
     */
    private static List<JEVisSample> mapToSortList(HashMap<DateTime, Double> map, JEVisAttribute sample) throws JEVisException {
        List<JEVisSample> list_sort = new ArrayList<JEVisSample>();
        Iterator iter = map.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry<DateTime, Double> entry = (Map.Entry) iter.next();
            JEVisSample d = sample.buildSample(entry.getKey(), entry.getValue());
            list_sort.add(d);
        }

        DataCompareTime comparator = new DataCompareTime();
        Collections.sort(list_sort, comparator);//sort the daten according to time

        return list_sort;
    }

    /*
     * this function is to store the result into data base
     */
    public void outputConfig(JEVisDataSource ds, long id, List<JEVisSample> result) throws JEVisException {
        JEVisObject obj = ds.getObject(id);//get the JEvis variable in data base
        JEVisAttribute result_val = obj.getAttribute("Value");//get all values and their times
        if (!result_val.getAllSamples().isEmpty()) {//check whether this variable any data has. If has,it can't directly cover the original data 
            result_val.deleteAllSample();//delete the original data(if the ID is wrong, the useful daten can also be deleted!!)
        }
        result_val.addSamples(result);//add the result into data base
    }

    // data row x data row
    //fertig
    @Override
    public List<JEVisSample> multiplication(List<JEVisSample> samples1, List<JEVisSample> samples2) throws ParseException, JEVisException {

//        List<JEVisSample> factor1 = new ArrayList();
//        List<JEVisSample> factor2 = new ArrayList();
        List<JEVisSample> realResult = new ArrayList();
//        factor1 = f1.getAllSamples();
//        factor2 = f2.getAllSamples();
        boolean find = false;
        DecimalFormat df = new DecimalFormat("####.000");
        for (JEVisSample o1 : samples1) {
            loop:
            for (JEVisSample o2 : samples2) {
                find = false;
                //compare the timestample,when equals, then multip, when not equals, then find next.
                if (o1.getTimestamp().equals(o2.getTimestamp())) {
                    JEVisSample a = o1.getAttribute().buildSample(((JEVisSample) o1).getTimestamp(), df.parse(df.format(((JEVisSample) o1).getValueAsDouble() * ((JEVisSample) o2).getValueAsDouble())));
                    realResult.add(a);
                    find = true;
                    break loop;
                }
            }
            if (find == false) {
                JEVisSample a = o1.getAttribute().buildSample(((JEVisSample) o1).getTimestamp(), df.parse(df.format(((JEVisSample) o1).getValueAsDouble())));
                realResult.add(a);
            }
        }
        return realResult;
    }

    //new added 
    //this function is to retain 3 decimal places after the decimal point
    public double formater(double x) {
        Locale.setDefault(Locale.ENGLISH);
        DecimalFormat df1 = new DecimalFormat("####.000");
        double result = Double.parseDouble(df1.format(x));
        return result;
    }

    // data row / data row
    @Override
    public List<JEVisSample> division(List<JEVisSample> samples1, List<JEVisSample> samples2) throws ParseException, JEVisException {

//        List<JEVisSample> temp1 = new ArrayList();
//        List<JEVisSample> temp2 = new ArrayList();
        List<JEVisSample> temp_result = new ArrayList();

        //check if the length of two data rows are same, if not, then division function will not be implenmented.         
        if (samples1.size() != samples2.size()) {
            System.out.println("the size of two samples are not same");
            return temp_result;
        }
//        temp1 = myAtt1.getAllSamples();
//        temp2 = myAtt2.getAllSamples();

        for (JEVisSample o1 : samples1) {
            loop:
            for (JEVisSample o2 : samples2) {
                //compare the timestample one by one,when equals, then multip, when not equals, then find next.
                if (o1.getTimestamp().equals(o2.getTimestamp())) {
                    if (o2.getValueAsDouble() != 0) {
                        JEVisSample a =o1.getAttribute().buildSample(o1.getTimestamp(), o1.getValueAsDouble() / o2.getValueAsDouble(), "finish");
                        temp_result.add(a);
                        break loop;
                    } else {
                        JEVisSample a = o1.getAttribute().buildSample(o1.getTimestamp(), 0, "wrong, the divider shouldn't be 0");
                        temp_result.add(a);
                    }
                }
            }
        }
        return temp_result;
    }


    @Override
    public double getAverageValue(List<JEVisSample> samples) throws JEVisException {
        double averagevalue = 0;
//        List<JEVisSample> temp = new ArrayList();
//        temp.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            averagevalue = averagevalue + o.getValueAsDouble();
        }
        averagevalue = averagevalue / samples.size();
        return averagevalue;
    }



    @Override
    public double getMaxValue(List<JEVisSample> samples) throws JEVisException {

        double max_value = 0;
//        List<JEVisSample> temp = new ArrayList();
//        temp.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            max_value = (max_value > o.getValueAsDouble()) ? max_value : o.getValueAsDouble();
        }
        return max_value;
    }
    
    

    //Calculate the Mean Deviation    
    @Override
    public double meanDeviation(List<JEVisSample> samples) throws JEVisException {
        double temp;
        double mean = 0;
        temp = this.getAverageValue(samples);
//        List<JEVisSample> tempp = new ArrayList();
//        tempp.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            mean = mean + Math.abs(o.getValueAsDouble() - temp);
        }
        mean=mean / samples.size();
        return mean;
    }
    

    //Add shifttime to original timeaxis. 
    @Override
    public List<JEVisSample> addShiftTime(List<JEVisSample> samples, int shiftTime) throws ParseException, JEVisException {

//        List<JEVisSample> temp = new ArrayList();
        List<JEVisSample> result = new ArrayList();
//        temp.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            JEVisSample a = o.getAttribute().buildSample(o.getTimestamp().plusSeconds(shiftTime), o.getValueAsDouble(), "finish");
            result.add(a);
        }
        return result;
    }
    
    //new added
    //Minus shifttime to original timeaxis. 
    public List<JEVisSample> minusShiftTime(List<JEVisSample> samples, int shiftTime) throws ParseException, JEVisException {

//        List<JEVisSample> temp = new ArrayList();
        List<JEVisSample> result = new ArrayList();
//        temp.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            JEVisSample a = o.getAttribute().buildSample(o.getTimestamp().minusSeconds(shiftTime),o.getValueAsDouble(), "Finish");
            result.add(a);
        }
        return result;
    }
    
    
 
    //only the value which smaller than setNumber can be stored.    
    @Override
    public List<JEVisSample> lowPassFilter(List<JEVisSample> samples, double setNumber) throws ParseException, JEVisException {
//        List<JEVisSample> temp_original_datarow = new ArrayList();
        List<JEVisSample> result = new ArrayList();
//        temp_original_datarow.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            if (o.getValueAsDouble() < setNumber) {
                JEVisSample a = o.getAttribute().buildSample(o.getTimestamp(), setNumber, "finish");
                result.add(a);
            } else {
                JEVisSample a = o.getAttribute().buildSample(((JEVisSample) o).getTimestamp(), ((JEVisSample) o).getValueAsDouble(), "finish");
                result.add(a);
            }
        }
        return result;
    }
      
 
    //this function will considers the period as no matter how long you give
    @Override
    public List<JEVisSample> derivation(List<JEVisSample> samples, int period) throws ParseException, JEVisException {
//        List<JEVisSample> temp_original_datarow = new ArrayList();
        List<JEVisSample> result = new ArrayList();
//        temp_original_datarow.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : samples) {
            if (result.size() < (samples.size() - 1)) {
                JEVisSample a = o.getAttribute().buildSample(o.getTimestamp(), ((samples.get(samples.indexOf(o) + 1).getValueAsDouble() - o.getValueAsDouble()) / period), "finish");
                result.add(a);
            }
        }
        return result;
    }
    
    //transform the data row back into the data row before using 
    //CumulativeDifferentialConverter function
    //little changed
    @Override
    public List<JEVisSample> differentialCumulativeConverter(List<JEVisSample> samples) throws JEVisException {
//        List<JEVisSample> originalDataRow = new ArrayList<JEVisSample>();
        List<JEVisSample> result = new ArrayList<JEVisSample>();
//        originalDataRow.addAll(myAtt1.getAllSamples());
        BigDecimal value = null;
          
        for (JEVisSample o : samples) {
            if (result.isEmpty()) {
                JEVisSample a = o.getAttribute().buildSample(o.getTimestamp(), o.getValue());
                result.add(a);
                value = new BigDecimal(o.getValueAsString());
            } else if (!result.isEmpty()) {
                value = new BigDecimal(o.getValueAsString()).add(value);
                JEVisSample temp2 = o.getAttribute().buildSample(o.getTimestamp(), value.doubleValue());
                result.add(temp2);
                value = new BigDecimal(result.get(result.size()-1).getValueAsString());
            }
        }
        return result;
    }
}
