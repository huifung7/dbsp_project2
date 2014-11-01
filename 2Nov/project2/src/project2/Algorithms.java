/**
 * @author axsun This code is provided solely for CZ4031 assignment 2. You
 * should provide implementation for the following three algorithms
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

        /*===============*/
        /*RELATION R*/
        /*===============*/
        ArrayList relRArrLst = sortRelation(relR_T, numIO);
        relR = (Relation) relRArrLst.get(0);
        numIOR = numIOR + (int) relRArrLst.get(1);
        numIO = numIO + numIOR;

        System.out.println("numIOR cost for Sorted RelR:\t" + numIOR);
        relR = toRelationSubList(relR);
        ArrayList relationRArrList = findRelationSmallest(relR, numIO);
        relR = (Relation) relationRArrList.get(0);

        ArrayList relationRSubList = spiltRelationToSubList(relR);
        relR.name = "RelR";
        //print relation. true to print, false not to print
        relR.printRelation(false, false);

        /*===============*/
        /*RELATION S*/
        /*===============*/
        ArrayList relSArrLst = sortRelation(relS_T, numIO);
        relS = (Relation) relSArrLst.get(0);
        numIOS = numIOS + (int) relSArrLst.get(1);
        numIO = numIO + numIOS;
        System.out.println("numIOS cost for Sorted RelS:\t" + numIOS);

        relS = toRelationSubList(relS);
        ArrayList relationSArrList = findRelationSmallest(relS, numIO);
        relS = (Relation) relationSArrList.get(0);
        ArrayList relationSSubList = spiltRelationToSubList(relS);
        relS.name = "RelS";
        //print relation. true to print, false not to print
        relS.printRelation(false, false);

        /*===============*/
        /*RELATION RS*/
        /*===============*/
        System.out.println("Total numIO cost for both RelR & RelS:\t" + numIO);
        //output the join of all tuples of R and S with common Y-value.
        numIO = numIO + mergeJRelationRS(relationRSubList, relationSSubList, relRS, numIO);
        
        //print the tuple result relRS
        relRS.printRelation(true, true);
        return numIO;
    }

    //Sort the Relation block based on the given size
    public ArrayList sortRelation(Relation paramRel, int paramNumIO) {
        int memorySizeValue = Setting.memorySize;
        int blockFactorValue = Setting.blockFactor;
        ArrayList returnAl = new ArrayList();
        int keyInt;
        String valueStr;
        RelationLoader relationLoaderObj = paramRel.getRelationLoader();
        List<Tuple> tupleList = new ArrayList();

        Relation tempRelationObj;
        if (paramRel.name.equalsIgnoreCase("RelR")) {
            tempRelationObj = new Relation(paramRel.name);
        } else {
            tempRelationObj = new Relation(paramRel.name);
        }
        Block newBlock = new Block();
        RelationWriter relationWriterObj = tempRelationObj.getRelationWriter();
        int blockSizeCount = 0;

        while (relationLoaderObj.hasNextBlock()) {
            Block[] blockArr = relationLoaderObj.loadNextBlocks(memorySizeValue);
            for (int b = 0; b < blockArr.length; b++) {
                Block blockObj = blockArr[b];
                paramNumIO++;
                if (blockObj != null) {
                    tupleList = new ArrayList();
                    //Loop through the un-sorted block tuple list
                    for (int bt = 0; bt < blockObj.tupleLst.size(); bt++) {
                        keyInt = blockObj.tupleLst.get(bt).key;
                        valueStr = blockObj.tupleLst.get(bt).value;
                        Tuple tempTupleObj = new Tuple(keyInt, valueStr);
                        tupleList.add(tempTupleObj);
                    }
                    //Using Collections.sort to the block tuple list
                    Collections.sort(tupleList, new ComparatorClass());
                    //Loop through the sorted tuple List
                    for (int t = 0; t < tupleList.size(); t++) {
                        newBlock.insertTuple((Tuple) tupleList.get(t));
                        blockSizeCount++;
                        if (blockSizeCount == blockFactorValue) {
                            relationWriterObj.writeBlock(newBlock);
                            newBlock = new Block();
                            blockSizeCount = 0;
                        }
                    }
                    if (newBlock.getNumTuples() != 0) {
                        relationWriterObj.writeBlock(newBlock);
                    }
                }
            }
        }

        paramRel = new Relation("paramRel");
        paramRel = tempRelationObj;
        returnAl.add(paramRel);
        returnAl.add(paramNumIO);
        return returnAl;
    }

    // Sort relation according to sublist of size M
    public Relation toRelationSubList(Relation paramRel) {
        int memorySizeValue = Setting.memorySize;
        int blockFactorValue = Setting.blockFactor;
        int memBlockValue = memorySizeValue * blockFactorValue;

        List<Tuple> tupleList = new ArrayList();
        RelationLoader relationLoaderObj = paramRel.getRelationLoader();
        Relation tempRelation = new Relation("tempRelation");
        RelationWriter relationWriterObj = tempRelation.getRelationWriter();

        Block blockObj;
        int keyInt;
        String valueStr;
        double blocksCount;
        Block newBlockObj;
        Tuple tupleObj;

        while (relationLoaderObj.hasNextBlock()) {
            Block[] tempBlocks = relationLoaderObj.loadNextBlocks(memorySizeValue);
            for (int tb = 0; tb < tempBlocks.length; tb++) {
                blockObj = tempBlocks[tb];
                if (blockObj != null) {
                    for (int bt = 0; bt < blockObj.tupleLst.size(); bt++) {
                        keyInt = blockObj.tupleLst.get(bt).key;
                        valueStr = blockObj.tupleLst.get(bt).value;
                        Tuple tempTm = new Tuple(keyInt, valueStr);
                        tupleList.add(tempTm);
                    }
                }
            }
            //Using Collections.sort to sort the tupleList of the current block
            Collections.sort(tupleList, new ComparatorClass());
            //Last block
            if (tupleList.size() < memBlockValue) {
                double calculateValue = tupleList.size() / (double) blockFactorValue;
                blocksCount = (Math.ceil(calculateValue));
                for (int bc = 0; bc < blocksCount; bc++) {
                    newBlockObj = new Block();
                    //last block
                    if (bc == blocksCount - 1) {
                        for (int i = 0; i < tupleList.size() % blockFactorValue; i++) {
                            if (bc != 0) {
                                int tupleCount = (bc * blockFactorValue) + i;
                                tupleObj = tupleList.get(tupleCount);
                                newBlockObj.insertTuple(tupleObj);
                            } else {
                                tupleObj = tupleList.get(i);
                                newBlockObj.insertTuple(tupleObj);
                            }
                        }
                    } else {    //not last block
                        for (int bf = 0; bf < blockFactorValue; bf++) {
                            if (bc != 0) {
                                int tupleCount = (bc * blockFactorValue) + bf;
                                tupleObj = tupleList.get(tupleCount);
                                newBlockObj.insertTuple(tupleObj);
                            } else {
                                tupleObj = tupleList.get(bf);
                                newBlockObj.insertTuple(tupleObj);
                            }
                        }
                    }
                    //write new block to relationwriter object
                    relationWriterObj.writeBlock(newBlockObj);
                }
            } else {
                blocksCount = tupleList.size() / blockFactorValue;
                for (int bc = 0; bc < blocksCount; bc++) {
                    newBlockObj = new Block();
                    for (int bf = 0; bf < blockFactorValue; bf++) {
                        if (bc != 0) {
                            int tupleCount = (bc * blockFactorValue) + bf;
                            tupleObj = tupleList.get(tupleCount);
                            newBlockObj.insertTuple(tupleObj);
                        } else {
                            tupleObj = tupleList.get(bf);
                            newBlockObj.insertTuple(tupleObj);
                        }
                    }

                    relationWriterObj.writeBlock(newBlockObj);
                }
            }
            tupleList.clear();
        }
        return tempRelation;
    }

    // Find smallest value
    public ArrayList findRelationSmallest(Relation paramRel, int paramNumIO) {
        int memorySizeValue = Setting.memorySize;
        int blockFactorValue = Setting.blockFactor;

        ArrayList returnRelArrayList = new ArrayList();
        List<Tuple> tupleList = new ArrayList();
        List<Block[]> blockList = new ArrayList();
        Block blockObj;
        Tuple sTupleObj;
        Tuple newTupleObj;
        Block outputBufferObj;
        Block compareBlockObj;
        Tuple checkTupleObj;

        boolean memoryBufferEmpty = true;
        Block[] memoryBufferArr = new Block[memorySizeValue];
        int blockCount;
        boolean isBlockEmpty = true;
        int numOfBlock = paramRel.getNumBlocks();
        RelationLoader relationLoaderObj = paramRel.getRelationLoader();
        paramRel = new Relation("paramRel");
        RelationWriter relationWriterObj = paramRel.getRelationWriter();

        tupleList.clear();

        //while in block load the next block based on the given memory size.
        while (relationLoaderObj.hasNextBlock()) {
            Block[] tempBlocks = relationLoaderObj.loadNextBlocks(memorySizeValue);
            blockList.add(tempBlocks);
        }

        List<Integer> blockCounts = new ArrayList();
        blockObj = new Block();
        for (int bs = 0; bs < blockList.size(); bs++) {
            for (int bt = 0; bt < blockList.get(bs)[0].getNumTuples(); bt++) {
                blockObj.tupleLst.add(blockList.get(bs)[0].tupleLst.get(bt));
            }
            memoryBufferArr[bs] = blockObj;
            paramNumIO++;
            blockObj = new Block();
            blockCount = 1;
            blockCounts.add(blockCount);
        }
        sTupleObj = new Tuple(memoryBufferArr[0].tupleLst.get(0).key, memoryBufferArr[0].tupleLst.get(0).value);
        outputBufferObj = new Block();
        int inLoopCount = 0;
        int outLoopCount = 0;

        for (int nb = 0; nb < numOfBlock; nb++) {
            for (int opBufferCount = 0; opBufferCount < blockFactorValue; opBufferCount++) {
                for (int outLoop = 0; outLoop < blockList.size(); outLoop++) {
                    if (memoryBufferArr[outLoop] != null) {
                        for (int inLoop = 0; inLoop < memoryBufferArr[outLoop].tupleLst.size(); inLoop++) {
                            if (memoryBufferArr[outLoop].tupleLst.get(inLoop) != null) {
                                newTupleObj = new Tuple(memoryBufferArr[outLoop].tupleLst.get(inLoop).key, memoryBufferArr[outLoop].tupleLst.get(inLoop).value);
                                if (sTupleObj.key > newTupleObj.key) {
                                    sTupleObj.key = newTupleObj.key;
                                    sTupleObj.value = newTupleObj.value;
                                    inLoopCount = inLoop;
                                    outLoopCount = outLoop;
                                }
                            }
                        }
                    } else {
                        //do nothing...
                    }
                }
                for (int bs = 0; bs < blockList.size(); bs++) {
                    Block check = memoryBufferArr[bs];
                    if (check != null) {
                        for (Tuple t : check.tupleLst) {
                            if (t != null) {
                                memoryBufferEmpty = false;
                            }
                        }
                    }
                }
                if (memoryBufferEmpty == true) {
                    break;
                } else {
                    outputBufferObj.insertTuple(sTupleObj);
                    sTupleObj = new Tuple(9999, "Test9999");
                    memoryBufferEmpty = true;
                }

                memoryBufferArr[outLoopCount].tupleLst.set(inLoopCount, null);

                //This step is to reload the blocks into the memory buffer 
                for (int chckMemBufferCount = 0; chckMemBufferCount < blockList.size(); chckMemBufferCount++) {
                    compareBlockObj = memoryBufferArr[chckMemBufferCount];
                    if (compareBlockObj != null) {
                        for (int ts = 0; ts < compareBlockObj.tupleLst.size(); ts++) {
                            checkTupleObj = memoryBufferArr[chckMemBufferCount].tupleLst.get(ts);
                            if (checkTupleObj != null) {
                                isBlockEmpty = false;
                            }
                        }
                        if (isBlockEmpty == true) {
                            int currentCount = blockCounts.get(chckMemBufferCount);
                            if (currentCount < memorySizeValue) {
                                memoryBufferArr[chckMemBufferCount] = blockList.get(chckMemBufferCount)[currentCount];
                                paramNumIO++;
                                currentCount++;
                                blockCounts.set(chckMemBufferCount, currentCount);
                            }
                        }
                        isBlockEmpty = true;
                    }
                }
            }

            relationWriterObj.writeBlock(outputBufferObj);
            paramNumIO++;
            outputBufferObj = new Block();
        }

        returnRelArrayList.add(paramRel);
        returnRelArrayList.add(paramNumIO);

        return returnRelArrayList;
    }

    // Spilt relation into sublists of size M
    public ArrayList spiltRelationToSubList(Relation paramRel) {
        int memorySizeValue = Setting.memorySize;

        List<Block> subBlockLists = new ArrayList();
        ArrayList subListsArrList = new ArrayList();
        RelationLoader rLoader = paramRel.getRelationLoader();
        while (rLoader.hasNextBlock()) {
            Block[] blockArr = rLoader.loadNextBlocks(memorySizeValue);
            //loop throught the block arr
            for (int b = 0; b < blockArr.length; b++) {
                Block blockObj = blockArr[b];
                if (blockObj != null) {
                    subBlockLists.add(blockObj);
                    if (subBlockLists.size() == memorySizeValue) {
                        subListsArrList.add(subBlockLists);
                        subBlockLists = new ArrayList();
                    }
                }
            }
        }
        subListsArrList.add(subBlockLists);
        return subListsArrList;
    }

    // Check and join tuples with the same key value
    public int mergeJRelationRS(ArrayList paramRelRSubList, ArrayList paramRelSSubList, Relation paramRelRS, int paramNumIO) {
        int memorySizeValue = Setting.memorySize;
        int blockFactorValue = Setting.blockFactor;
        
        RelationWriter rsWriter = paramRelRS.getRelationWriter();

        Block newBlockObj = new Block();
        int numOfRelRTuple = 0;
        int numOfRelSTuple = 0;
        int totalTuples = 0;

        // Get counts of total number of tuples in Relation R and S
        for (Object rb : paramRelRSubList) {
            List<Block> blockList = (List<Block>) rb;
            for (Block blockObj : blockList) {
                numOfRelRTuple = numOfRelRTuple + blockObj.tupleLst.size();
            }
        }
        for (Object sb : paramRelSSubList) {
            List<Block> blockList = (List<Block>) sb;
            for (Block blockObj : blockList) {
                numOfRelSTuple = numOfRelSTuple + blockObj.tupleLst.size();
            }
        }
        totalTuples = numOfRelSTuple + numOfRelRTuple;
//        System.out.println("**totalTuples:\t"+ totalTuples);

        while (totalTuples > 0) {
            for (int relRInt = 0; relRInt < paramRelRSubList.size(); relRInt++) {
                ArrayList subListRArrList = (ArrayList) paramRelRSubList.get(relRInt);
                for (int r = 0; r < subListRArrList.size(); r++) {
                    Block relRBlockBuffer = (Block) subListRArrList.get(r);
                    for (int rbb = 0; rbb < relRBlockBuffer.tupleLst.size(); rbb++) {
                        Tuple relRTupleObj = relRBlockBuffer.tupleLst.get(rbb);

                        for (int relSInt = 0; relSInt < paramRelSSubList.size(); relSInt++) {
                            ArrayList subListS = (ArrayList) paramRelSSubList.get(relSInt);
                            for (int s = 0; s < subListS.size(); s++) {
                                Block relSBlockBuffer = (Block) subListS.get(s);
                                for (int sbb = 0; sbb < relSBlockBuffer.tupleLst.size(); sbb++) {
                                    Tuple relSTupleObj = relSBlockBuffer.tupleLst.get(sbb);

                                    totalTuples--;
                                    // Checks if value is equal then perform join
                                    if (relRTupleObj.key == relSTupleObj.key) {
                                        JointTuple joinTupleObj = new JointTuple(relRTupleObj, relSTupleObj);
                                        // Checks if block is full
                                        if (!newBlockObj.insertTuple(joinTupleObj)) {
                                            // Write block to output if its full
                                            rsWriter.writeBlock(newBlockObj);
                                            // Increment IO cost
                                            paramNumIO++;
                                            // Declare new block to store new tuple
                                            newBlockObj = new Block();
                                            // Insert new tuple
                                            newBlockObj.insertTuple(joinTupleObj);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Checks that if the current new block have any existing tuples in it
            // and it's not written to the relation yet
            // If so then write it to the relation
            // If not then do nothing
            if (newBlockObj.getNumTuples() != 0) {
                rsWriter.writeBlock(newBlockObj);
                paramNumIO++;
            }
        }
        return paramNumIO;
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
        // Insert your test cases here!
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
//            Algorithms.examples();
        Algorithms.testCases();
    }
}