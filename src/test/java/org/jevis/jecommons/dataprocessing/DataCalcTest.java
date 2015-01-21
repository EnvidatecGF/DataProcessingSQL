///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package org.jevis.jecommons.dataprocessing;
//
//import java.util.HashMap;
//import java.util.List;
//import org.jevis.api.JEVisAttribute;
//import org.jevis.api.JEVisDataSource;
//import org.jevis.api.JEVisException;
//import org.jevis.api.JEVisObject;
//import org.jevis.api.JEVisSample;
//import org.jevis.api.sql.JEVisDataSourceSQL;
//import org.joda.time.DateTime;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;
//import org.junit.After;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import static org.junit.Assert.*;
//
///**
// *
// * @author gf
// */
//public class DataCalcTest {
//    JEVisDataSource ds;
//    JEVisObject obj;
//    
//    public DataCalcTest() {
//    }
//    
//    @BeforeClass
//    public static void setUpClass() {
//    }
//    
//    @AfterClass
//    public static void tearDownClass() {
//    }
//    
//    @Before
//    public void setUp() throws JEVisException {
//        ds = new JEVisDataSourceSQL("192.168.2.55", "3306", "jevis", "jevis", "jevistest", null, null);
//        ds.connect("Sys Admin", "jevis");
//    }
//    
//    @After
//    public void tearDown() {
//    }
//
//    /**
//     * Test of addition method, of class DataCalc.
//     */
//    @Test
//    public void testAddition_JEVisAttribute_double() throws Exception {
//        System.out.println("Addition");
//        
//        obj =ds.getObject(895l);
//        JEVisAttribute sample = obj.getAttribute("Value");
//        
//        double value = 1;
//        
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1013l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(1053l);
//        calc.outputConfig(ds, 1053, calc.addition(sample, value));
//        JEVisAttribute sample5= obj.getAttribute("Value");
//        List<JEVisSample> result = sample5.getAllSamples();
//
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of addition method, of class DataCalc.
//     */
//    @Test
//    public void testAddition_JEVisAttribute_JEVisAttribute() throws Exception {
//        System.out.println("addition");
//        
//        obj =ds.getObject(895l);
//        JEVisAttribute sample1= obj.getAttribute("Value");
//        
//        obj =ds.getObject(896l);
//        JEVisAttribute sample2= obj.getAttribute("Value");
//        
//        obj =ds.getObject(1085l);
//        JEVisAttribute sample3= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1054l);
//        JEVisAttribute sample4= obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample4.getAllSamples();
//        
//        obj =ds.getObject(1053l);
//        calc.outputConfig(ds, 1053, calc.addition(sample1, sample2));
//        JEVisAttribute sample5= obj.getAttribute("Value");
//        List<JEVisSample> result = sample5.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//        
//        obj =ds.getObject(1087l);
//        sample4= obj.getAttribute("Value");
//        expResult = sample4.getAllSamples();
//        
//        obj =ds.getObject(1086l);
//        calc.outputConfig(ds, 1086, calc.addition(sample1, sample3));
//        sample5= obj.getAttribute("Value");
//        result = sample5.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of boundaryFilter method, of class DataCalc.
//     */
//    @Test
//    public void testBoundaryFilter() throws Exception {
//        System.out.println("boundaryFilter");
//        
//        obj =ds.getObject(899l);
//        JEVisAttribute myAtt= obj.getAttribute("Value");
//        
//        DataCalc calc=new DataCalc();
//
//        obj =ds.getObject(1055l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(922l);
//        calc.outputConfig(ds, 922l, calc.boundaryFilter(myAtt,3,-4));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of cumulativeDifferentialConverter method, of class DataCalc.
//     */
//    @Test
//    public void testCumulativeDifferentialConverter() throws Exception {
//        System.out.println("cumulativeDifferentialConverter");
//        
//        obj =ds.getObject(917l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1058l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(918l);
//        calc.outputConfig(ds, 918l, calc.cumulativeDifferentialConverter(sample));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of highPassFilter method, of class DataCalc.
//     */
//    @Test
//    public void testHighPassFilter_JEVisAttribute_double() throws Exception {
//        System.out.println("highPassFilter");
//        
//        obj =ds.getObject(899l);
//        JEVisAttribute myAtt= obj.getAttribute("Value");
//        
//        double boundary = 3;
//        DataCalc calc=new DataCalc();
//
//        obj =ds.getObject(1056l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(923l);
//        calc.outputConfig(ds, 923l, calc.highPassFilter(myAtt,boundary));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of highPassFilter method, of class DataCalc.
//     */
//    @Test
//    public void testHighPassFilter_3args() throws Exception {
//        System.out.println("highPassFilter");
//        
//        JEVisObject obj1 =ds.getObject(899l);
//        JEVisAttribute myAtt= obj1.getAttribute("Value");
//        
//        double boundary = 3;
//        double fill_value = 0;
//        DataCalc calc=new DataCalc();
//
//        JEVisObject obj2 =ds.getObject(1057l);
//        JEVisAttribute sample2 = obj2.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(924l);
//        calc.outputConfig(ds, 924l, calc.highPassFilter(myAtt,boundary,fill_value));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of integration method, of class DataCalc.
//     */
//    @Test
//    public void testIntegration() throws Exception {
//        System.out.println("integration");
//        
//        obj =ds.getObject(892l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        
//        Double expResult = 255489.75;
//        Double result = calc.integration(sample);
//        
//        assertEquals(expResult, result);
//    }
//
//    /**
//     * Test of intervalAlignment method, of class DataCalc.
//     */
//    @Test
//    public void testIntervalAlignment() throws Exception {
//        System.out.println("intervalAlignment");
//        
//        obj =ds.getObject(908l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
//        DateTime begin_time = formatter.parseDateTime("2014-11-25 10:00:00");
//        
//        int period_s = 900;
//        int deviation_s = 30;
//        DataCalc calc=new DataCalc();
//        
//        obj =ds.getObject(1059l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(909l);
//        calc.outputConfig(ds, 909l, calc.intervalAlignment(sample, begin_time, period_s, deviation_s));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of linearInterpolation method, of class DataCalc.
//     */
//    @Test
//    public void testLinearInterpolation() throws Exception {
//        System.out.println("linearInterpolation");
//        
//        obj =ds.getObject(920l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        int insert_num = 2;
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1061l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(921l);
//        calc.outputConfig(ds, 921l, calc.linearInterpolation(sample, insert_num));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        } 
//    }
//
//    /**
//     * Test of linearScaling method, of class DataCalc.
//     */
//    @Test
//    public void testLinearScaling() throws Exception {
//        System.out.println("linearScaling");
//        
//        obj =ds.getObject(905l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        double proportion = 2;
//        double b = 1;
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1066l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(906l);
//        calc.outputConfig(ds, 906l, calc.linearScaling(sample, proportion, b));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of median method, of class DataCalc.
//     */
//    @Test
//    public void testMedian() throws Exception {
//        System.out.println("median");
//        
//        obj =ds.getObject(925l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        Double expResult = -0.5735;
//        Double result = calc.median(sample);
//        
//        assertEquals(expResult, result);
//    }
//
//    /**
//     * Test of mergeValues method, of class DataCalc.
//     */
//    @Test
//    public void testMergeValues() throws Exception {
//        System.out.println("mergeValues");
//        
//        obj =ds.getObject(911l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss");
//        DateTime begin_time = formatter.parseDateTime("2014-11-25 10:00:00");
//        
//        int period_s = 900;
//        int meg_num = 3;
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1071l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(912l);
//        calc.outputConfig(ds, 912l, calc.mergeValues(sample, begin_time, period_s, meg_num));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of precisionFilter method, of class DataCalc.
//     */
//    @Test
//    public void testPrecisionFilter() throws Exception {
//        System.out.println("precisionFilter");
//        
//        obj =ds.getObject(901l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        double percent = 0.1;
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1072l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(903l);
//        calc.outputConfig(ds, 903l, calc.precisionFilter(sample, percent));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of sortByTime method, of class DataCalc.
//     */
//    @Test
//    public void testSortByTime() {
//        System.out.println("sortByTime");
//        JEVisAttribute sample = null;
//        DataCalc instance = new DataCalc();
//        List expResult = null;
//        List result = instance.sortByTime(sample);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of sortByValue method, of class DataCalc.
//     */
//    @Test
//    public void testSortByValue() {
//        System.out.println("sortByValue");
//        JEVisAttribute sample = null;
//        DataCalc instance = new DataCalc();
//        List expResult = null;
//        List result = instance.sortByValue(sample);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of splitValues method, of class DataCalc.
//     */
//    @Test
//    public void testSplitValues() throws Exception {
//        System.out.println("splitValues");
//        
//        obj =ds.getObject(914l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        int period_s = 900;
//        int seg_num = 3;
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1073l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(915l);
//        calc.outputConfig(ds, 915l, calc.splitValues(sample, period_s, seg_num));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of subtraction method, of class DataCalc.
//     */
//    @Test
//    public void testSubtraction_JEVisAttribute_double() throws Exception {
//        System.out.println("subtraction");
//        
//        obj =ds.getObject(1076l);
//        JEVisAttribute sample = obj.getAttribute("Value");
//        
//        double value = 3;
//        
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1080l);
//        JEVisAttribute sample2 = obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(1078l);
//        calc.outputConfig(ds, 915l, calc.subtraction(sample, value));
//        JEVisAttribute sample3 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample3.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of subtraction method, of class DataCalc.
//     */
//    @Test
//    public void testSubtraction_JEVisAttribute_JEVisAttribute() throws Exception {
//        System.out.println("subtraction");
//        
//        obj =ds.getObject(1076l);
//        JEVisAttribute sample1= obj.getAttribute("Value");
//        
//        obj =ds.getObject(1075l);
//        JEVisAttribute sample2= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1079l);
//        JEVisAttribute sample3= obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample3.getAllSamples();
//        
//        obj =ds.getObject(1077l);
//        calc.outputConfig(ds, 1077l, calc.subtraction(sample1, sample2));
//        JEVisAttribute sample4 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample4.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of valueAllMinimum method, of class DataCalc.
//     */
//    @Test
//    public void testValueAllMinimum() throws Exception {
//        System.out.println("valueAllMinimum");
//        
//        obj =ds.getObject(925l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        
//        obj =ds.getObject(1082l);
//        JEVisAttribute sample2= obj.getAttribute("Value");
//        List<JEVisSample> expResult = sample2.getAllSamples();
//        
//        obj =ds.getObject(1081l);
//        calc.outputConfig(ds, 1081l, calc.valueAllMinimum(sample));
//        JEVisAttribute sample4 = obj.getAttribute("Value");
//        List<JEVisSample> result =sample4.getAllSamples();
//        
//        for (int i=0;i<result.size();i++){
//            assertEquals(expResult.get(i).getValue(),result.get(i).getValue());
//            assertEquals(expResult.get(i).getTimestamp(),result.get(i).getTimestamp());
//        }
//    }
//
//    /**
//     * Test of valueMinimum method, of class DataCalc.
//     */
//    @Test
//    public void testValueMinimum_JEVisAttribute() throws Exception {
//        System.out.println("valueMinimum");
//        
//        obj =ds.getObject(925l);
//        JEVisAttribute sample= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        
//        double expResult = -18.124;
//        double result = calc.valueMinimum(sample);
//        
//        assertEquals(expResult, result, 0.0);
//    }
//
//    /**
//     * Test of valueMinimum method, of class DataCalc.
//     */
//    @Test
//    public void testValueMinimum_double_double() throws Exception {
//        System.out.println("valueMinimum");
//        double sample1 = -2;
//        double sample2 = 0.0;
//        DataCalc calc = new DataCalc();
//        double expResult = -2;
//        double result = calc.valueMinimum(sample1, sample2);
//        assertEquals(expResult, result,0.0);
//        
//    }
//
//    /**
//     * Test of valueMinimum method, of class DataCalc.
//     */
//    @Test
//    public void testValueMinimum_JEVisAttribute_JEVisAttribute() throws Exception {
//        System.out.println("valueMinimum");
//        
//        obj =ds.getObject(925l);
//        JEVisAttribute sample1= obj.getAttribute("Value");
//        
//        obj =ds.getObject(925l);
//        JEVisAttribute sample2= obj.getAttribute("Value");
//        
//        DataCalc calc = new DataCalc();
//        double expResult = -18.124;
//        double result = calc.valueMinimum(sample1, sample2);
//        
//        assertEquals(expResult, result, 0.0);
//    }
//
//    /**
//     * Test of valueMinimum method, of class DataCalc.
//     */
//    @Test
//    public void testValueMinimum_3args() throws Exception {
//        System.out.println("valueMinimum");
//        obj =ds.getObject(925l);
//        JEVisAttribute sample1= obj.getAttribute("Value");
//        
//        obj =ds.getObject(925l);
//        JEVisAttribute sample2= obj.getAttribute("Value");
//        
//        double value = -20;
//        DataCalc calc = new DataCalc();
//        
//        double expResult = -20;
//        double result = calc.valueMinimum(sample1, sample2, value);
//        assertEquals(expResult, result, 0.0);
//    }
//
////    /**
////     * Test of listToMap method, of class DataCalc.
////     */
////    @Test
////    public void testListToMap() throws Exception {
////        System.out.println("listToMap");
////        JEVisAttribute sample = null;
////        HashMap expResult = null;
////        HashMap result = DataCalc.listToMap(sample);
////        assertEquals(expResult, result);
////        // TODO review the generated test code and remove the default call to fail.
////        fail("The test case is a prototype.");
////    }
////
////    /**
////     * Test of mapToSortList method, of class DataCalc.
////     */
////    @Test
////    public void testMapToSortList() throws Exception {
////        System.out.println("mapToSortList");
////        HashMap<DateTime, Double> map = null;
////        JEVisAttribute sample = null;
////        List expResult = null;
////        List result = DataCalc.mapToSortList(map, sample);
////        assertEquals(expResult, result);
////        // TODO review the generated test code and remove the default call to fail.
////        fail("The test case is a prototype.");
////    }
////
////    /**
////     * Test of outputConfig method, of class DataCalc.
////     */
////    @Test
////    public void testOutputConfig() throws Exception {
////        System.out.println("outputConfig");
////        JEVisDataSource ds = null;
////        long id = 0L;
////        List<JEVisSample> result_2 = null;
////        DataCalc instance = new DataCalc();
////        instance.outputConfig(ds, id, result_2);
////        // TODO review the generated test code and remove the default call to fail.
////        fail("The test case is a prototype.");
////    }
//}