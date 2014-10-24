/**
 * @author axsun This code is provided solely for CZ4031 assignment 2. This set
 * of code shall NOT be redistributed. You should provide implementation for the
 * three algorithms declared in this class.
 */
package project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import project2.Relation.RelationLoader;
import project2.Relation.RelationWriter;

public class Algorithms {

    /**
     * Sort the relation using Setting.memorySize buffers of memory
     *
     * @param rel is the relation to be sorted.
     * @return the number of IO cost (in terms of reading and writing blocks)
     */
    public int mergeSortRelation(Relation rel) {
        int numIO = 0;

        //Insert your code here!
        System.out.println(rel.getNumBlocks());

        return numIO;
    }

    /**
     * Join relations relR and relS using Setting.memorySize buffers of memory
     * to produce the result relation relRS
     *
     * @param relR is one of the relation in the join
     * @param relS is the other relation in the join
     * @param relRS is the result relation of the join
     * @return the number of IO cost (in terms of reading and writing blocks)
     */
    public int hashJoinRelations(Relation relR, Relation relS, Relation relRS) {
        int numIO = 0;

        //Insert your code here!
        return numIO;
    }

    /**
     * Join relations relR and relS using Setting.memorySize buffers of memory
     * to produce the result relation relRS
     *
     * @param relR is one of the relation in the join
     * @param relS is the other relation in the join
     * @param relRS is the result relation of the join
     * @return the number of IO cost (in terms of reading and writing blocks)
     */
    public int refinedSortMergeJoinRelations(Relation relR, Relation relS, Relation relRS) {
        int numIO = 0;
        int numIOR = 0;
        int numIOS = 0;
        //Insert your code here!
        relRS = new Relation("RelRS");

        //Take in the Relation parameter object and put in temp relation object
        //for relR and relS
        Relation relR_T = relR;
        Relation relS_T = relS;

        relR = new Relation("RelR");
        relS = new Relation("RelS");

        /**
         * 
         * ==========================================
         * PHASE 1: CREATE SORTED SUBLISTS OF SIZE M
         * USING Y AS THE SORT KEY,
         * FOR BOTH R AND S
         * ===========================================
         * 
         **/
        
        /*===============*/
        /*SORT RELATION R*/
        /*CALL sortRelation method*/
        /*===============*/
        ArrayList relRArrLst = sortRelation(relR_T, numIOR);
        relR = (Relation) relRArrLst.get(0);
        numIOR = (int) relRArrLst.get(1);
        numIO = numIO + numIOR;
        System.out.println("numIOR cost for Sorted RelR:\t" + numIOR);
        
        /*===============*/
        /*SORT RELATION S*/
        /*===============*/
        ArrayList relSArrLst = sortRelation(relS_T, numIOS);
        relS = (Relation) relSArrLst.get(0);
        numIOS = (int) relSArrLst.get(1);
        numIO = numIO + numIOS;
        System.out.println("numIOS cost for Sorted RelS:\t" + numIOS);
        
        System.out.println("Total numIO cost for both RelR & RelS:\t" + numIO);
        
        /*====================================================*/
        /*CREATE SORTED SUBLIST OF SIZE 'M' for RELATION R & S*/
        /*====================================================*/
        relR = createSubListRelation(relR);
        relS = createSubListRelation(relS);
        
        /**
         * 
         * ==========================================================================
         * PHASE 2: BRING FIRST BLOCK OF EACH SUBLIST INTO BUFFER
         * REPEATEDLY FIND THE LEAST Y-VALUE y AMONG THE FIRST AVAILABLE TUPLES OF ALL SUBLISTS
         * IDENTIFY ALL TUPLES IN RELATION R & S THAT HAVE y
         * OUTPUT THE JOIN OF ALL TUPLES OF R WITH ALL OF S WITH COMMON Y-VALUE
         * IF THE BUFFER FOR ONE OF THE SUBLIST IS EXHAUSTED, REPLENISH
         * ===========================================================================
         * 
         **/
        
        
        return numIO;
    }

    public ArrayList sortRelation(Relation paramRel, int paramNumIO) {
        int memorySizeValue = Setting.memorySize;
        //put into arraylist
        ArrayList sortRelationArrList = new ArrayList();
        int keyInt;
        String valueStr;

        RelationLoader relationLoaderObj = paramRel.getRelationLoader();
        List<Tuple> tupleList = new ArrayList();
        Relation tempRelationObj;

        //check the relation name and create a relation object
        if (paramRel.name.equalsIgnoreCase("RelS")) {
            tempRelationObj = new Relation(paramRel.name);
        } else {
            tempRelationObj = new Relation(paramRel.name);
        }

        //put into blocks by using relationWriter
        Block newBlockObj = new Block();
        RelationWriter relationWriterObj = tempRelationObj.getRelationWriter();
        int blockSizeCount = 0;

        //loop to load the next block
        while (relationLoaderObj.hasNextBlock()) {
            Block[] blockArr = relationLoaderObj.loadNextBlocks(memorySizeValue);
            for (int i = 0; i < blockArr.length; i++) {
                Block blockObj = blockArr[i];
                paramNumIO++;
                if (blockObj != null) {
                    for (int j = 0; j < blockObj.tupleLst.size(); j++) {
                        keyInt = blockObj.tupleLst.get(j).key;
                        valueStr = blockObj.tupleLst.get(j).value;
                        //put the keyInt and valueStr into TupleObj
                        //and add into the tuple list
                        Tuple tupleObj = new Tuple(keyInt, valueStr);
                        tupleList.add(tupleObj);
                    }
                    //once done, sort the tuple list using collection.sort
                    Collections.sort(tupleList, new ComparatorClass());
                    for (int q = 0; q < tupleList.size(); q++) {
                        newBlockObj.insertTuple((Tuple) tupleList.get(q));
                        blockSizeCount++;
                        if (blockSizeCount == Setting.blockFactor) {
                            relationWriterObj.writeBlock(newBlockObj);
                            newBlockObj = new Block();
                            blockSizeCount = 0;
                        }
                    }
                    if (newBlockObj.getNumTuples() != 0) {
                        relationWriterObj.writeBlock(newBlockObj);
                    }
                }
            }
        }

        paramRel = new Relation("paramRel");
        paramRel = tempRelationObj;
        sortRelationArrList.add(paramRel);
        sortRelationArrList.add(paramNumIO);

        return sortRelationArrList;
    }
    
    public Relation createSubListRelation (Relation paramRel){
        int memorySizeValue = Setting.memorySize;
        int blockFactorValue = Setting.blockFactor;
        Relation tempRelationObj = new Relation("tempRelationObj");
        List<Tuple> tupleList = new ArrayList();
        RelationLoader relationLoaderObj = paramRel.getRelationLoader();
        RelationWriter relationWriteObj = tempRelationObj.getRelationWriter();
        Block blockObj;
        int keyValue;
        String strValue;
        double blocksCount;
        Block newBlockObj;
        Tuple tupleObj;
        
        while (relationLoaderObj.hasNextBlock()) {
            Block[] blockArr = relationLoaderObj.loadNextBlocks(memorySizeValue);
            for (int i = 0; i < blockArr.length; i++) {
                blockObj = blockArr[i];
                if (blockObj != null) {
                    for (int j = 0; j < blockObj.tupleLst.size(); j++) {
                        keyValue = blockObj.tupleLst.get(j).key;
                        strValue = blockObj.tupleLst.get(j).value;
                        Tuple tempTupleObj = new Tuple(keyValue, strValue);
                        tupleList.add(tempTupleObj);
                    }
                }
            }
            Collections.sort(tupleList, new ComparatorClass());
            //Check the tuple size if it less than the total size from Setting.
            int bM = blockFactorValue * memorySizeValue;
            if (tupleList.size() < bM) {
                double calculateValue = tupleList.size() / (double) blockFactorValue;
                blocksCount = (Math.ceil(calculateValue));
                for (int q = 0; q < blocksCount; q++) {
                    newBlockObj = new Block();
                    if (q == blocksCount - 1) {
                        int modSize = tupleList.size() % blockFactorValue;
                        for (int k = 0; k < modSize; k++) {
                            if (q != 0) {
                                int tupleCount = (q * blockFactorValue) + k;
                                tupleObj = tupleList.get(tupleCount);
                                newBlockObj.insertTuple(tupleObj);
                            } else {
                                tupleObj = tupleList.get(k);
                                newBlockObj.insertTuple(tupleObj);
                            }
                        }
                    } else {
                        for (int bf = 0; bf < Setting.blockFactor; bf++) {
                            if (q != 0) {
                                int tupleCount = (q * Setting.blockFactor) + bf;
                                tupleObj = tupleList.get(tupleCount);
                                newBlockObj.insertTuple(tupleObj);
                            } else {
                                tupleObj = tupleList.get(bf);
                                newBlockObj.insertTuple(tupleObj);
                            }
                        }
                    }

                    relationWriteObj.writeBlock(newBlockObj);
                }
            } else {
                blocksCount = tupleList.size() / blockFactorValue;
                for (int c = 0; c < blocksCount; c++) {
                    newBlockObj = new Block();
                    for (int a = 0; a < blockFactorValue; a++) {
                        if (c != 0) {
                            int tupleCount = (c * blockFactorValue) + a;
                            tupleObj = tupleList.get(tupleCount);
                            newBlockObj.insertTuple(tupleObj);
                        } else {
                            tupleObj = tupleList.get(a);
                            newBlockObj.insertTuple(tupleObj);
                        }
                    }
                    relationWriteObj.writeBlock(newBlockObj);
                }
            }
            tupleList.clear();
        }

        return tempRelationObj;
    }

    // Comparator Class - for comparing tuple object key and the subsequent key.
    class ComparatorClass implements Comparator<Tuple> {

        @Override
        public int compare(Tuple tupleObj1, Tuple tupleObj2) {
            int firstKey = tupleObj1.key;
            int secondKey = tupleObj2.key;
            int result;
            if (firstKey > secondKey) {
                result = 1;
            } else if (firstKey < secondKey) {
                result = -1;
            } else {
                result = 0;
            }
            return result;
        }
    }

    /**
     * Example usage of classes.
     */
    public static void examples() {

        /*Populate relations*/
        System.out.println("---------Populating two relations----------");
        Relation relR = new Relation("RelR");
        int numTuples = relR.populateRelationFromFile("RelR.txt");
        System.out.println("Relation RelR contains " + numTuples + " tuples.");
        Relation relS = new Relation("RelS");
        numTuples = relS.populateRelationFromFile("RelS.txt");
        System.out.println("Relation RelS contains " + numTuples + " tuples.");
        System.out.println("---------Finish populating relations----------\n\n");

        /*Print the relation */
        System.out.println("---------Printing relations----------");
        relR.printRelation(true, true);
        relS.printRelation(false, false);
        System.out.println("---------Finish printing relations----------\n\n");

        /*Example use of RelationLoader*/
        System.out.println("---------Loading relation RelR using RelationLoader----------");
        RelationLoader rLoader = relR.getRelationLoader();
        while (rLoader.hasNextBlock()) {
            System.out.println("--->Load at most 7 blocks each time into memory...");
            Block[] blocks = rLoader.loadNextBlocks(7);
            //print out loaded blocks 
            for (Block b : blocks) {
                if (b != null) {
                    b.print(false);
                }
            }
        }
        System.out.println("---------Finish loading relation RelR----------\n\n");

        /*Example use of RelationWriter*/
        System.out.println("---------Writing to relation RelS----------");
        RelationWriter sWriter = relS.getRelationWriter();
        rLoader.reset();
        if (rLoader.hasNextBlock()) {
            System.out.println("Writing the first 7 blocks from RelR to RelS");
            System.out.println("--------Before writing-------");
            relR.printRelation(false, false);
            relS.printRelation(false, false);

            Block[] blocks = rLoader.loadNextBlocks(7);
            for (Block b : blocks) {
                if (b != null) {
                    sWriter.writeBlock(b);
                }
            }
            System.out.println("--------After writing-------");
            relR.printRelation(false, false);
            relS.printRelation(false, false);
        }
    }

    /**
     * Testing cases.
     */
    public static void testCases() {
        Relation relRS = new Relation("RelRS");
        Algorithms algoObj = new Algorithms();

        /*Populate relations*/
        System.out.println("---------Populating two relations----------");
        Relation relR = new Relation("RelR");
        int numTuples = relR.populateRelationFromFile("RelR.txt");
        System.out.println("Relation RelR contains " + numTuples + " tuples.");
        Relation relS = new Relation("RelS");
        numTuples = relS.populateRelationFromFile("RelS.txt");
        System.out.println("Relation RelS contains " + numTuples + " tuples.");
        System.out.println("---------Finish populating relations----------\n\n");

        // Insert your test cases here!
        System.out.println("\n---------refinedSortMergeJoinRelations----------");
        // Call refine sort merge join relations method and
        // pass in relation R and S and the relation RS to store the results of join
        int refinedSMJIO = algoObj.refinedSortMergeJoinRelations(relR, relS, relRS);
        // Print out Refined Sort Merge Join IO cost
        System.out.println("Refined Sort Merge Join I/O Cost: " + refinedSMJIO);
        System.out.println("---------Finish refinedSortMergeJoinRelations----------\n");

    }

    /**
     * This main method provided for testing purpose
     *
     * @param arg
     */
    public static void main(String[] arg) {
//        Algorithms.examples();
        Algorithms.testCases();
    }
}
