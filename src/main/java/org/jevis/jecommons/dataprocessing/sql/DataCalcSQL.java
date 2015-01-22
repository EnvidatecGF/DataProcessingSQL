/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.jecommons.dataprocessing.sql;

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
import org.jevis.jecommons.dataprocessing.DataCalc;
import org.joda.time.DateTime;

/**
 *
 * @author gf
 */
public class DataCalcSQL implements DataCalc{

    @Override
    public List<JEVisSample> addition(JEVisAttribute sample, double value) throws JEVisException {
        List<JEVisSample> sample_add = new ArrayList<JEVisSample>();

        Locale.setDefault(Locale.US); //avoid the different form of decimal point
        
        BigDecimal num; //creat a "BigDeciaml" variable
        BigDecimal val=new BigDecimal(value); //creat a "BigDeciaml" variable and convert "value" to BigDecimal
        for (JEVisSample row : sample.getAllSamples()) {
            num=new BigDecimal(row.getValueAsString()); //the convertion is to avoid the number of decimals,because of double

            JEVisSample samp=sample.buildSample(row.getTimestamp(), num.add(val).doubleValue());//convert the result and it's time to typ JEVisSample
            sample_add.add(samp);//put the result into a list
        }

        return sample_add;
    }

    @Override
    public List<JEVisSample> addition(List<JEVisAttribute> attributes) throws JEVisException {
        
        HashMap<DateTime, String> timestamps = new HashMap<DateTime, String>();//timestamps is to add all timestamps of every attribute(data row)
        HashMap<DateTime, Double> result_map = new HashMap<DateTime, Double>();
        
        for(JEVisAttribute att:attributes){
            timestamps.putAll(listToMap(att));//put all timestamps in, as a scalar
        }
        for (Map.Entry entry : timestamps.entrySet()) {
            BigDecimal sum=new BigDecimal(0); 
            BigDecimal value;
            for(JEVisAttribute att:attributes){
                HashMap<DateTime, String> map_att = listToMap(att); //convert every data row to a Map for convenience of seeking
                if(map_att.containsKey(entry.getKey())){ //if every data row contains the timestamps in "timestamps",then calculate
                    value=new BigDecimal(map_att.get(entry.getKey()));
                    sum=sum.add(value);
                }
                result_map.put((DateTime)entry.getKey(), sum.doubleValue());//connect the result to it's time 
            }
        }

        return mapToSortList(result_map, attributes.get(0));
    }
    
    @Override
    public List<JEVisSample> addition(JEVisAttribute sample1, JEVisAttribute sample2) throws JEVisException {
        HashMap<DateTime, String> map1 = listToMap(sample1); //convert sample1 to a Map
        HashMap<DateTime, String> map2 = listToMap(sample2); //convert sample2 to a Map
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

        return mapToSortList(result_map, sample1);
    }
                 
    /*
    the combination of high pass filter und low pass filter
    The input parameter „delete“ decides, whether the improper values 
    will be deleted or replaced by  the upper limit and  the lower limit.
    */
    @Override
    public List<JEVisSample> boundaryFilter(JEVisAttribute sample, double boundary_up, double boundary_low, boolean delete) throws JEVisException {
        List<JEVisSample> sample_bf = new ArrayList<JEVisSample>();//creat a List to put the result
        JEVisSample newsample;

        if(delete){//delete=true,don't store the ineligible value and its'time point
            for (JEVisSample circ : sample.getAllSamples()) {
            if (circ.getValueAsDouble() <= boundary_up && circ.getValueAsDouble() >= boundary_low) {
                newsample = sample.buildSample(circ.getTimestamp(), circ.getValueAsDouble());
                sample_bf.add(newsample);
            }
        }
        }else{//delete=false,replace the ineligible value with upper limit and low limit
            for (JEVisSample circ : sample.getAllSamples()) {
            if (circ.getValueAsDouble() > boundary_up) {
                newsample = sample.buildSample(circ.getTimestamp(), boundary_up);
            } else if (circ.getValueAsDouble() < boundary_low) {
                newsample = sample.buildSample(circ.getTimestamp(), boundary_low);
            } else {
                newsample = sample.buildSample(circ.getTimestamp(), circ.getValueAsDouble());
            }
            sample_bf.add(newsample);
        }
        }
        return sample_bf;
    }

    /*
    
    */
    @Override
    public List<JEVisSample> cumulativeDifferentialConverter(JEVisAttribute sample) throws JEVisException {
        List<JEVisSample> sample_cd = new ArrayList<JEVisSample>();//creat a List to put the result
        DateTime time;
        BigDecimal value = null;
        int count = 0;

        for (JEVisSample row : sample.getAllSamples()) {
            count++;//to judge the first data

            if (count == 1) {
                value = new BigDecimal(row.getValueAsString());//convert the first value to BigDecimal for the preparation of calculation
                sample_cd.add(sample.buildSample(row.getTimestamp(), row.getValueAsDouble()));//directly put the first data into the result List
            } else {
                value = new BigDecimal(row.getValueAsString()).subtract(value);//the last value subtracts the previous one
                time = row.getTimestamp();
                sample_cd.add(sample.buildSample(time, value.doubleValue()));
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
    public List<JEVisSample> highPassFilter(JEVisAttribute sample, double boundary) throws JEVisException {
        List<JEVisSample> sample_hpf = new ArrayList<JEVisSample>();//creat a List to put the result
        JEVisSample newsample;//to get the eligible values(JEVisSample)

        for (JEVisSample circ : sample.getAllSamples()) {
            if (circ.getValueAsDouble() > boundary) {
                newsample = sample.buildSample(circ.getTimestamp(), circ.getValueAsDouble());
            } else {
                newsample = sample.buildSample(circ.getTimestamp(), boundary);
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
    public List<JEVisSample> highPassFilter(JEVisAttribute sample, double boundary, double fill_value) throws JEVisException {
        List<JEVisSample> sample_hpf = new ArrayList<JEVisSample>();//creat a List to put the result
        JEVisSample newsample;//to get the eligible values(JEVisSample)


        for (JEVisSample circ : sample.getAllSamples()) {
//            list.setTime(circ.getTimestamp());
            if (circ.getValueAsDouble() > boundary) {
                newsample = sample.buildSample(circ.getTimestamp(), circ.getValueAsDouble());
            } else {
                newsample = sample.buildSample(circ.getTimestamp(), fill_value);
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
    public double integration(JEVisAttribute sample) throws JEVisException {
        BigDecimal integration =new BigDecimal("0");
        BigDecimal[] time_interval = new BigDecimal[sample.getAllSamples().size() - 1];  //the interval between every two time points

        for (int i = 0; i < sample.getAllSamples().size() - 1; i++) {
            //the unit of time_interval is second
            time_interval[i] = new BigDecimal(sample.getAllSamples().get(i + 1).getTimestamp().getSecondOfDay()).subtract(new BigDecimal(sample.getAllSamples().get(i).getTimestamp().getSecondOfDay()));
            if (time_interval[i].equals(new BigDecimal("0"))) {
                throw new IllegalArgumentException("X must bemontotonic. A duplicate " + "x-value was found");
            }
        }

        for (int i = 0; i < sample.getAllSamples().size() - 1; i++) {
            BigDecimal num1=new BigDecimal(sample.getAllSamples().get(i).getValueAsString());
            BigDecimal num2=new BigDecimal(sample.getAllSamples().get(i + 1).getValueAsString());
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
    @Override
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
    public List<JEVisSample> intervalAlignment(JEVisAttribute sample, int period_s, int deviation_s) throws JEVisException {//, DateTime begin_time
        List<JEVisSample> sample_ia = new ArrayList<JEVisSample>();//creat a List to put the result
        DateTime right_time;
        
        for(JEVisSample row : sample.getAllSamples()){
            int remainder=row.getTimestamp().getSecondOfDay()% period_s;//get the remainder of the sampled time/period
            if(remainder <= period_s && remainder >= (period_s-deviation_s)){//the sampled time is earlier than the right time or equals to the right time
                right_time=row.getTimestamp().plusSeconds(period_s-remainder);
                sample_ia.add(sample.buildSample(right_time, row.getValue()));
            }else if(remainder >= 0 && remainder <= deviation_s){//the sampled is later than the right time or the right time
                right_time=row.getTimestamp().minusSeconds(remainder);
                sample_ia.add(sample.buildSample(right_time, row.getValue()));
            }
        }

        return sample_ia;
    }

    /*
     * interpolation the whole data row, from begin to end
     */
    @Override
    public List<JEVisSample> linearInterpolation(JEVisAttribute sample, int insert_num) throws JEVisException {
        List<JEVisSample> sample_i = new ArrayList<JEVisSample>();//creat a List to put the result
        double value;
        DateTime time;

        double[] dx = new double[sample.getAllSamples().size() - 1];//to get the differences between every two adjacent times
        double[] dy = new double[sample.getAllSamples().size() - 1];//to get the differences between every two adjacent vaules

        Locale.setDefault(Locale.US);
        
        //calculate the differences
        for (int i = 0; i < sample.getAllSamples().size() - 1; i++) {
            dx[i] = sample.getAllSamples().get(i + 1).getTimestamp().getSecondOfDay() - sample.getAllSamples().get(i).getTimestamp().getSecondOfDay();
            if (dx[i] == 0) {
                throw new IllegalArgumentException("X must bemontotonic. A duplicate " + "x-value was found");
            }
            dy[i] = sample.getAllSamples().get(i + 1).getValueAsDouble()-sample.getAllSamples().get(i).getValueAsDouble();
        }

        for (int i = 0; i < sample.getAllSamples().size()-1; i++) {
            for (int j = 0; j < insert_num + 1; j++) {
                int nb = insert_num + 1;
                JEVisSample data;
                time = sample.getAllSamples().get(i).getTimestamp().plusSeconds((int) (dx[i] * j / nb));
                value = sample.getAllSamples().get(i).getValueAsDouble() + dy[i]*j/nb;
                DecimalFormat format=new DecimalFormat("0.#####");
                data = sample.buildSample(time, Double.parseDouble(format.format(value)));
                sample_i.add(data);
            }
        }
        sample_i.add(sample.buildSample(sample.getLatestSample().getTimestamp(), sample.getLatestSample().getValue()));
        return sample_i;
    }
    
        /*
     * interpolation in a range
     */
    @Override
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
    public List<JEVisSample> linearScaling(JEVisAttribute sample, double proportion, double b) throws JEVisException {
        List<JEVisSample> sample_ls = new ArrayList<JEVisSample>();//creat a List to put the result
        BigDecimal p=new BigDecimal(proportion);//convert to the BigDecimal to avoid the number of decimals,because of double
        BigDecimal bb=new BigDecimal(b);//convert to the BigDecimal to avoid the avoid the number of decimals,because of double

        for (JEVisSample circ : sample.getAllSamples()) {
            sample_ls.add(sample.buildSample(circ.getTimestamp(), new BigDecimal(circ.getValueAsString()).multiply(p).add(bb).doubleValue()));//calculation

        }
        return sample_ls;
    }
    
    /*
    look for the median of one data row(JEVis variable). 
    */
    @Override
    public double median(JEVisAttribute sample) throws JEVisException {
        Double median;
        List<JEVisSample> list_med = sample.getAllSamples();//get all daten,are not sorted according to value yet

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
     * The input parameter „begin_time“ is the theoretic begin time(the first sampled time) of the data row. 
     * The input parameter „period_s“ should be time(period) and it's unit is  second. 
     * The last input parameter „meg_num“ means, how many sampled value will be merged.
     * (millinsecond for year is too lang,already beyound the int,so the smallest unit of time is here second.)
     */
    @Override
    public List<JEVisSample> mergeValues(JEVisAttribute sample, DateTime begin_time, int period_s, int meg_num) throws JEVisException {  //seg_num:the number of merge
        List<JEVisSample> sample_mv = new ArrayList<JEVisSample>();//creat a List to put the result
        BigDecimal sum= new BigDecimal("0");
        DateTime time = begin_time;//get the theoretic begin time
        DateTime time_l = new DateTime();

        for (JEVisSample row : sample.getAllSamples()) {
            if (row.getTimestamp().isBefore(time.plusSeconds(period_s * meg_num))) {
                sum = sum.add(new BigDecimal(row.getValueAsString()));
                time_l = time.plusSeconds(period_s * (meg_num - 1));//time_l=row.getTime();
            } else {
                sample_mv.add(sample.buildSample(time_l, sum.doubleValue()));
                time = time.plusSeconds(period_s * meg_num);
                sum = new BigDecimal("0");
                sum = sum.add(new BigDecimal(row.getValueAsString()));
                time_l = time.plusSeconds(period_s * (meg_num - 1));//time_l=row.getTime();
            }
        }
        sample_mv.add(sample.buildSample(time_l, sum.doubleValue()));//.doubleValue()
        return sample_mv;
    }

    /*
    delete the value,that is not bigger or smaller than it's previous value in one percentage value. 
    The inputparameter „percent“ is the percentage value, which is decided by enduser.
    */
    @Override
    public List<JEVisSample> precisionFilter(JEVisAttribute sample, double percent) throws JEVisException {
        List<JEVisSample> sample_pf = new ArrayList<JEVisSample>();//create a List to put the result
        List<JEVisSample> sa = sample.getAllSamples();//get all daten
        double perc;//get the calculated percent

        JEVisSample s = sample.buildSample(sa.get(0).getTimestamp(), sa.get(0).getValueAsDouble());//get the first value
        //s kann nicht getValueAsDouble() anrufen    (Double)      
        sample_pf.add(s);

        for (int j = 1; j < sa.size(); j++) {
//            System.out.println(s.getValue());
//            System.out.println(sa.get(j).getValueAsDouble());
            perc = Math.abs(sa.get(j).getValueAsDouble() - (Double) s.getValue()) / (Double) s.getValue();//calculate the percent of every two adjacent value
            if (perc >= percent) {
                s = sample.buildSample(sa.get(j).getTimestamp(), sa.get(j).getValueAsDouble());//get the next eligible value,to be calculated next time
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
    public List<JEVisSample> sortByTime(JEVisAttribute sample,int order) {
        List<JEVisSample> sample_sbt = sample.getAllSamples();

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
    public List<JEVisSample> sortByValue(JEVisAttribute sample,int order) {
        List<JEVisSample> sample_sbv = sample.getAllSamples();

        DataCompareValue comparator = new DataCompareValue(order);
        Collections.sort(sample_sbv, comparator);

        return sample_sbv;
    }
    
    /*
     * only split the value as average, it's not komplete
     */
    @Override
    public List<JEVisSample> splitValues(JEVisAttribute sample, int period_s, int seg_num) throws JEVisException {  //seg_num:the number of segmentation
        List<JEVisSample> sample_sv = new ArrayList<JEVisSample>();
        //int count = seg_num;
        BigDecimal value_n = new BigDecimal("0");
        DateTime time;

        for (JEVisSample row : sample.getAllSamples()) {
            for (int i = 1; i < (seg_num + 1); i++) {
                time = row.getTimestamp().minusSeconds((seg_num + 1 - i) * period_s / (seg_num + 1));
                value_n = new BigDecimal(row.getValueAsString()).divide(new BigDecimal(seg_num + 1), 10,RoundingMode.HALF_UP);//if can't be divided with no remainder,then keep 10 decimals
                sample_sv.add(sample.buildSample(time, value_n.doubleValue()));
            }

            sample_sv.add(sample.buildSample(row.getTimestamp(), value_n.doubleValue()));
        }
        return sample_sv;
    }
    
    /*
    every value of the data row minus one value
    */
    @Override
    public List<JEVisSample> subtraction(JEVisAttribute sample, double value) throws JEVisException {
        List<JEVisSample> sample_sub = new ArrayList<JEVisSample>();
        
        Locale.setDefault(Locale.US);//avoid the different form of decimal point
        BigDecimal num;
        BigDecimal v=new BigDecimal(value);//creat a "BigDeciaml" variable and convert "value" to BigDecimal
        for (JEVisSample row : sample.getAllSamples()) {
            num=new BigDecimal(row.getValueAsString());//the convertion is to avoid the number of decimals,because of double
            JEVisSample samp=sample.buildSample(row.getTimestamp(), num.subtract(v).doubleValue());
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
    public List<JEVisSample> subtraction(JEVisAttribute sample1, JEVisAttribute sample2) throws JEVisException {
        HashMap<DateTime, String> map1 = listToMap(sample1);//convert sample1 to a Map
        HashMap<DateTime, String> map2 = listToMap(sample2);//convert sample2 to a Map
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

        return mapToSortList(result_map, sample1);//convert the map to List,and return
    }

    /*
     * output all minimum values with their time in the data row
     */
    @Override
    public List<JEVisSample> valueAllMinimum(JEVisAttribute sample) throws JEVisException {
        List<JEVisSample> multiple_min = new ArrayList<JEVisSample>();
        
        //use valueMinimum(JEVisAttribute sample) to search the smallest value in the data roll
        double min=valueMinimum(sample);

        //with the smallest vallue,that is found before, find all smallest values in the data roll
        for (JEVisSample circ : sample.getAllSamples()) {
            if (min==circ.getValueAsDouble()) {
                multiple_min.add(sample.buildSample(circ.getTimestamp(), circ.getValueAsDouble()));//creat a variable of typ JeVisSample and put it into List
            }
        }
        return multiple_min;
    }

    /*
     * output only the minimum value in one data row
     */
    @Override
    public double valueMinimum(JEVisAttribute sample) throws JEVisException {
        double min = 0;

        min = sample.getAllSamples().get(0).getValueAsDouble();
        //search the smallest value in the data roll
        for (JEVisSample circ1 : sample.getAllSamples()) {
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
    public double valueMinimum(List<JEVisAttribute> attributes) throws JEVisException {

        double min;
        double min_result=valueMinimum(attributes.get(0));//find the minimum value in first data row as the first min_result
        for(JEVisAttribute att:attributes){
            min=valueMinimum(att);//find the minimum value in the rest data rows and compare it to min_result
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
    public double valueMinimum(List<JEVisAttribute> attributes, double value) throws JEVisException {
        double min = valueMinimum(attributes);//use valueMinimum(List<JEVisAttribute> attributes) to find the minimum value of multiple data rows
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
    public List<DateTime> findGap(JEVisAttribute sample, int period_s, int deviation_s) throws JEVisException {
//        System.out.println("If the function \"intervalAlignment\" is not used before the function \"findGap\",there will be a wrong result!!");

        List<JEVisSample> list = intervalAlignment(sample, period_s, deviation_s);//use "intervalAlignment" to eliminate the deviation of sampled time
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
    public static HashMap<DateTime, String> listToMap(JEVisAttribute sample) throws JEVisException {
        HashMap<DateTime, String> map = new HashMap<DateTime, String>();

        for (JEVisSample d : sample.getAllSamples()) {
            map.put(d.getTimestamp(), d.getValueAsString());
        }

        return map;
    }

    /*
     * this function is to simplify the calculation of other functions. 
     * This function converts the map to JEVis variable(JEVisSample)
     * and sort the daten according to time. 
     */
    public static List<JEVisSample> mapToSortList(HashMap<DateTime, Double> map, JEVisAttribute sample) throws JEVisException {
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
    public List<JEVisSample> multiplication(JEVisAttribute f1, JEVisAttribute f2) throws ParseException, JEVisException {

        List<JEVisSample> factor1 = new ArrayList();
        List<JEVisSample> factor2 = new ArrayList();
        List<JEVisSample> realResult = new ArrayList();
        factor1 = f1.getAllSamples();
        factor2 = f2.getAllSamples();
        boolean find = false;
        DecimalFormat df = new DecimalFormat("####.000");
        for (Object o1 : factor1) {
            loop:
            for (Object o2 : factor2) {
                find = false;
                //compare the timestample,when equals, then multip, when not equals, then find next.
                if (((JEVisSample) o1).getTimestamp().equals(((JEVisSample) o2).getTimestamp())) {
                    JEVisSample sample = f1.buildSample(((JEVisSample) o1).getTimestamp(), df.parse(df.format(((JEVisSample) o1).getValueAsDouble() * ((JEVisSample) o2).getValueAsDouble())));
                    realResult.add(sample);
                    find = true;
                    break loop;
                }
            }
            if (find == false) {
                JEVisSample sample = f1.buildSample(((JEVisSample) o1).getTimestamp(), df.parse(df.format(((JEVisSample) o1).getValueAsDouble())));
                realResult.add(sample);
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
    public List<JEVisSample> division(JEVisAttribute myAtt1, JEVisAttribute myAtt2) throws ParseException, JEVisException {

        List<JEVisSample> temp1 = new ArrayList();
        List<JEVisSample> temp2 = new ArrayList();
        List<JEVisSample> temp_result = new ArrayList();

        //check if the length of two data rows are same, if not, then division function will not be implenmented.         
        if (myAtt1.getAllSamples().size() != myAtt2.getAllSamples().size()) {
            System.out.println("the size of two samples are not same");
            return temp_result;
        }
        temp1 = myAtt1.getAllSamples();
        temp2 = myAtt2.getAllSamples();

        for (Object o1 : temp1) {
            loop:
            for (Object o2 : temp2) {
                //compare the timestample one by one,when equals, then multip, when not equals, then find next.
                if (((JEVisSample) o1).getTimestamp().equals(((JEVisSample) o2).getTimestamp())) {
                    if (((JEVisSample) o2).getValueAsDouble() != 0) {
                        JEVisSample sample = myAtt1.buildSample(((JEVisSample) o1).getTimestamp(), ((JEVisSample) o1).getValueAsDouble() / ((JEVisSample) o2).getValueAsDouble(), "finish");
                        temp_result.add(sample);
                        break loop;
                    } else {
                        JEVisSample sample = myAtt1.buildSample(((JEVisSample) o1).getTimestamp(), 0, "wrong, the divider shouldn't be 0");
                        temp_result.add(sample);
                    }
                }
            }
        }
        return temp_result;
    }


    @Override
    public double getAverageValue(JEVisAttribute myAtt1) throws JEVisException {
        double averagevalue = 0;
        List<JEVisSample> temp = new ArrayList();
        temp.addAll(myAtt1.getAllSamples());
        for (Object o : temp) {
            averagevalue = averagevalue + ((JEVisSample) o).getValueAsDouble();
        }
        averagevalue = averagevalue / myAtt1.getAllSamples().size();
        return averagevalue;
    }



    @Override
    public double getMaxValue(JEVisAttribute myAtt1) throws JEVisException {

        double max_value = 0;
        List<JEVisSample> temp = new ArrayList();
        temp.addAll(myAtt1.getAllSamples());
        for (Object o : temp) {
            max_value = (max_value > ((JEVisSample) o).getValueAsDouble()) ? max_value : ((JEVisSample) o).getValueAsDouble();
        }
        return max_value;
    }
    
    

    //Calculate the Mean Deviation    
    @Override
    public double meanDeviation(JEVisAttribute myAtt1) throws JEVisException {
        double temp;
        double mean = 0;
        temp = this.getAverageValue(myAtt1);
        List<JEVisSample> tempp = new ArrayList();
        tempp.addAll(myAtt1.getAllSamples());
        for (Object o : tempp) {
            mean = mean + Math.abs(((JEVisSample) o).getValueAsDouble() - temp);
        }
        mean=mean / myAtt1.getAllSamples().size();
        return mean;
    }
    

    //Add shifttime to original timeaxis. 
    @Override
    public List<JEVisSample> addShiftTime(JEVisAttribute myAtt1, int shiftTime) throws ParseException, JEVisException {

        List<JEVisSample> temp = new ArrayList();
        List<JEVisSample> result = new ArrayList();
        temp.addAll(myAtt1.getAllSamples());
        for (Object o : temp) {
            JEVisSample sample = myAtt1.buildSample(((JEVisSample) o).getTimestamp().plusSeconds(shiftTime), ((JEVisSample) o).getValueAsDouble(), "finish");
            result.add(sample);
        }
        return result;
    }
    
    //new added
    //Minus shifttime to original timeaxis. 
    public List<JEVisSample> minusShiftTime(JEVisAttribute myAtt1, int shiftTime) throws ParseException, JEVisException {

        List<JEVisSample> temp = new ArrayList();
        List<JEVisSample> result = new ArrayList();
        temp.addAll(myAtt1.getAllSamples());
        for (Object o : temp) {
            JEVisSample sample = myAtt1.buildSample(((JEVisSample) o).getTimestamp().minusSeconds(shiftTime), ((JEVisSample) o).getValueAsDouble(), "Finish");
            result.add(sample);
        }
        return result;
    }
    
    
 
    //only the value which smaller than setNumber can be stored.    
    @Override
    public List<JEVisSample> lowPassFilter(JEVisAttribute myAtt1, double setNumber) throws ParseException, JEVisException {
        List<JEVisSample> temp_original_datarow = new ArrayList();
        List<JEVisSample> result = new ArrayList();
        temp_original_datarow.addAll(myAtt1.getAllSamples());
        for (Object o : temp_original_datarow) {
            if (((JEVisSample) o).getValueAsDouble() < setNumber) {
                JEVisSample sample = myAtt1.buildSample(((JEVisSample) o).getTimestamp(), setNumber, "finish");
                result.add(sample);
            } else {
                JEVisSample sample = myAtt1.buildSample(((JEVisSample) o).getTimestamp(), ((JEVisSample) o).getValueAsDouble(), "finish");
                result.add(sample);
            }
        }
        return result;
    }
      
 
    //this function will considers the period as no matter how long you give
    @Override
    public List<JEVisSample> derivation(JEVisAttribute myAtt1, int period) throws ParseException, JEVisException {
        List<JEVisSample> temp_original_datarow = new ArrayList();
        List<JEVisSample> result = new ArrayList();
        temp_original_datarow.addAll(myAtt1.getAllSamples());
        for (Object o : temp_original_datarow) {
            if (result.size() < (temp_original_datarow.size() - 1)) {
                JEVisSample sample = myAtt1.buildSample(((JEVisSample) o).getTimestamp(), ((temp_original_datarow.get(temp_original_datarow.indexOf(o) + 1).getValueAsDouble() - ((JEVisSample) o).getValueAsDouble()) / period), "finish");
                result.add(sample);
            }
        }
        return result;
    }
    
    //transform the data row back into the data row before using 
    //CumulativeDifferentialConverter function
    //little changed
    @Override
    public List<JEVisSample> differentialCumulativeConverter(JEVisAttribute myAtt1) throws JEVisException {
        List<JEVisSample> originalDataRow = new ArrayList<JEVisSample>();
        List<JEVisSample> result = new ArrayList<JEVisSample>();
        originalDataRow.addAll(myAtt1.getAllSamples());
        for (JEVisSample o : originalDataRow) {
            if (result.isEmpty()) {
                JEVisSample temp = myAtt1.buildSample(o.getTimestamp(), o.getValue());
                result.add(temp);
            } else if (!result.isEmpty()) {
                JEVisSample temp2 = myAtt1.buildSample(o.getTimestamp(), (Double.parseDouble(result.get(result.size() - 1).getValueAsString()) + o.getValueAsDouble()));
                result.add(temp2);
            }
        }
        return result;
    }

}
