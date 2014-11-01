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

        ArrayList relationRSubList = spiltRelation(relR);
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
        ArrayList relationSSubList = spiltRelation(relS);
        relS.name = "RelS";
        //print relation. true to print, false not to print
        relS.printRelation(false, false);

        /*===============*/
        /*RELATION RS*/
        /*===============*/
        System.out.println("Total numIO cost for both RelR & RelS:\t" + numIO);
        //output the join of all tuples of R and S with common Y-value.
        numIO = numIO + outputRelationRS(relationRSubList, relationSSubList, relRS, numIO);
        relRS.printRelation(false, false);
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
    public Relation toRelationSubList(Relation inRel) {
        List<Tuple> tupleList = new ArrayList();
        RelationLoader rLoader2 = inRel.getRelationLoader();
        Relation tempRelation = new Relation("tempRelation");
        RelationWriter tempWriter = tempRelation.getRelationWriter();
        Block b;
        int key;
        String value;
        double blocksCount;
        Block newBlock;
        Tuple T;
        while (rLoader2.hasNextBlock()) {
            Block[] tempBlocks = rLoader2.loadNextBlocks(Setting.memorySize);
            for (int z = 0; z < tempBlocks.length; z++) {
                b = tempBlocks[z];
                if (b != null) {
                    for (int i = 0; i < b.tupleLst.size(); i++) {
                        key = b.tupleLst.get(i).key;
                        value = b.tupleLst.get(i).value;
                        Tuple tempTm = new Tuple(key, value);
                        tupleList.add(tempTm);
                    }
                }
            }

            Collections.sort(tupleList, new ComparatorClass());
            //check for the last case block
            if (tupleList.size() < (Setting.blockFactor * Setting.memorySize)) {
                double calculateValue = tupleList.size() / (double) Setting.blockFactor;
                blocksCount = (Math.ceil(calculateValue));
                for (int c = 0; c < blocksCount; c++) {
                    newBlock = new Block();
                    //check for the very last block
                    if (c == blocksCount - 1) {
                        for (int a = 0; a < tupleList.size() % Setting.blockFactor; a++) {
                            if (c != 0) {
                                int tupleCount = (c * Setting.blockFactor) + a;
                                T = tupleList.get(tupleCount);
                                newBlock.insertTuple(T);
                            } else {
                                T = tupleList.get(a);
                                newBlock.insertTuple(T);
                            }
                        }
                    } else {
                        for (int a = 0; a < Setting.blockFactor; a++) {
                            if (c != 0) {
                                int tupleCount = (c * Setting.blockFactor) + a;
                                T = tupleList.get(tupleCount);
                                newBlock.insertTuple(T);
                            } else {
                                T = tupleList.get(a);
                                newBlock.insertTuple(T);
                            }
                        }
                    }

                    tempWriter.writeBlock(newBlock);
                }
            } else {
                blocksCount = tupleList.size() / Setting.blockFactor;
                for (int c = 0; c < blocksCount; c++) {
                    newBlock = new Block();
                    for (int a = 0; a < Setting.blockFactor; a++) {
                        if (c != 0) {
                            int tupleCount = (c * Setting.blockFactor) + a;
                            T = tupleList.get(tupleCount);
                            newBlock.insertTuple(T);
                        } else {
                            T = tupleList.get(a);
                            newBlock.insertTuple(T);
                        }
                    }

                    tempWriter.writeBlock(newBlock);
                }
            }
            tupleList.clear();
        }
        //tempRelation.printRelation(true, true);
        return tempRelation;
    }

    // Find smallest value
    public ArrayList findRelationSmallest(Relation tempRelation, int numIO) {
        ArrayList returnArrayList = new ArrayList();
        List<Tuple> tupleList = new ArrayList();
        List<Block[]> blockList = new ArrayList();
        Block tempBlock;
        Tuple smallest;
        Tuple newTuple;
        Block outputBuffer;
        Block compareBlock;
        Tuple checkTuple;
        boolean memoryBufferEmpty = true;
        Block[] memoryBuffer = new Block[Setting.memorySize];
        int blockCount;
        boolean blockEmpty = true;
        int numOfBlock = tempRelation.getNumBlocks();
        RelationLoader rLoader3 = tempRelation.getRelationLoader();
        tempRelation = new Relation("tempRelation");
        RelationWriter tempWriter = tempRelation.getRelationWriter();
        tupleList.clear();
        while (rLoader3.hasNextBlock()) {
            Block[] tempBlocks = rLoader3.loadNextBlocks(Setting.memorySize);
            blockList.add(tempBlocks);
        }

        List<Integer> blockCounts = new ArrayList();
        tempBlock = new Block();
        for (int q = 0; q < blockList.size(); q++) {
            for (int l = 0; l < blockList.get(q)[0].getNumTuples(); l++) {
                tempBlock.tupleLst.add(blockList.get(q)[0].tupleLst.get(l));
            }
            memoryBuffer[q] = tempBlock;
            numIO++;
            tempBlock = new Block();
            blockCount = 1;
            blockCounts.add(blockCount);
        }
        smallest = new Tuple(memoryBuffer[0].tupleLst.get(0).key, memoryBuffer[0].tupleLst.get(0).value);
        outputBuffer = new Block();
        int innerLoopCount = 0, outerLoopCount = 0;

        for (int w = 0; w < numOfBlock; w++) {

            //This step is to find the smallest buffer and out to the output buffer
            for (int outPutBufferCount = 0; outPutBufferCount < Setting.blockFactor; outPutBufferCount++) {
                for (int outerLoop = 0; outerLoop < blockList.size(); outerLoop++) {
                    if (memoryBuffer[outerLoop] != null) {
                        for (int innerLoop = 0; innerLoop < memoryBuffer[outerLoop].tupleLst.size(); innerLoop++) {
                            if (memoryBuffer[outerLoop].tupleLst.get(innerLoop) != null) {
                                newTuple = new Tuple(memoryBuffer[outerLoop].tupleLst.get(innerLoop).key, memoryBuffer[outerLoop].tupleLst.get(innerLoop).value);
                                if (smallest.key > newTuple.key) {
                                    smallest.key = newTuple.key;
                                    smallest.value = newTuple.value;
                                    innerLoopCount = innerLoop;
                                    outerLoopCount = outerLoop;
                                }
                            }
                        }
                    } else {
                    }
                }
                for (int y = 0; y < blockList.size(); y++) {
                    Block check = memoryBuffer[y];
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
                    outputBuffer.insertTuple(smallest);
                    smallest = new Tuple(999999, "Test");
                    memoryBufferEmpty = true;
                }

                memoryBuffer[outerLoopCount].tupleLst.set(innerLoopCount, null);

                //This step is to reload the blocks into the memory buffer 
                for (int checkMemoryBufferCount = 0; checkMemoryBufferCount < blockList.size(); checkMemoryBufferCount++) {
                    compareBlock = memoryBuffer[checkMemoryBufferCount];
                    if (compareBlock != null) {
                        for (int u = 0; u < compareBlock.tupleLst.size(); u++) {
                            checkTuple = memoryBuffer[checkMemoryBufferCount].tupleLst.get(u);
                            if (checkTuple != null) {
                                blockEmpty = false;
                            }
                        }
                        if (blockEmpty == true) {
                            int currentCount = blockCounts.get(checkMemoryBufferCount);
                            if (currentCount < Setting.memorySize) {
                                memoryBuffer[checkMemoryBufferCount] = blockList.get(checkMemoryBufferCount)[currentCount];
                                numIO++;
                                currentCount++;
                                blockCounts.set(checkMemoryBufferCount, currentCount);
                            }

                        }
                        blockEmpty = true;
                    }

                }
            }

            tempWriter.writeBlock(outputBuffer);
            numIO++;
            outputBuffer = new Block();

        }
//            tempRelation.printRelation(false, false);
        returnArrayList.add(tempRelation);
        returnArrayList.add(numIO);
        return returnArrayList;
    }

    // Spilt relation into sublists of size M
    public ArrayList spiltRelation(Relation inRel) {
        List<Block> subLists = new ArrayList();
        ArrayList subListsAl = new ArrayList();
        RelationLoader rLoader = inRel.getRelationLoader();
        while (rLoader.hasNextBlock()) {
            Block[] blocks = rLoader.loadNextBlocks(Setting.memorySize);
            // Iterate through blocks
            for (int j = 0; j < blocks.length; j++) {
                Block b = blocks[j];
                if (b != null) {
                    subLists.add(b);
                    if (subLists.size() == Setting.memorySize) {
                        subListsAl.add(subLists);
                        subLists = new ArrayList();
                    }
                }
            }
        }
        subListsAl.add(subLists);
        return subListsAl;
    }

    // Check and join tuples with the same key value
    public int outputRelationRS(ArrayList rSubList, ArrayList sSubList, Relation relRS, int numIO) {
        RelationWriter rsWriter = relRS.getRelationWriter();

        Block newBlock = new Block();
        int numOfTupleS = 0;
        int numOfTupleR = 0;
        int totalTuples = 0;

        // Get counts of total number of tuples in Relation R and S
        for (Object tb : rSubList) {
            List<Block> lb = (List<Block>) tb;
            for (Block b : lb) {
                for (Tuple t : b.tupleLst) {
                    numOfTupleS++;
                }
            }
        }
        for (Object tb : sSubList) {
            List<Block> lb = (List<Block>) tb;
            for (Block b : lb) {
                for (Tuple t : b.tupleLst) {
                    numOfTupleR++;
                }
            }
        }
        totalTuples = numOfTupleS + numOfTupleR;

        while (totalTuples > 0) {
            for (int i = 0; i < rSubList.size(); i++) {
                ArrayList subListR = (ArrayList) rSubList.get(i);
                for (int j = 0; j < subListR.size(); j++) {
                    Block rBuffer = (Block) subListR.get(j);
//                        numIO++;
                    for (int k = 0; k < rBuffer.tupleLst.size(); k++) {
                        Tuple rTuple = rBuffer.tupleLst.get(k);

                        for (int l = 0; l < sSubList.size(); l++) {
                            ArrayList subListS = (ArrayList) sSubList.get(l);
                            for (int p = 0; p < subListS.size(); p++) {
                                Block sBuffer = (Block) subListS.get(p);
                                for (int q = 0; q < sBuffer.tupleLst.size(); q++) {
                                    Tuple sTuple = sBuffer.tupleLst.get(q);

                                    totalTuples--;
                                    // Checks if value is equal then perform join
                                    if (rTuple.key == sTuple.key) {
                                        JointTuple jt = new JointTuple(rTuple, sTuple);
                                        // Checks if block is full
                                        if (!newBlock.insertTuple(jt)) {
                                            // Write block to output if its full
                                            rsWriter.writeBlock(newBlock);
                                            // Increment IO cost
                                            numIO++;
                                            // Declare new block to store new tuple
                                            newBlock = new Block();
                                            // Insert new tuple
                                            newBlock.insertTuple(jt);
                                        }
                                    }

                                }
                            }
                        }
//                            numIO++;
                    }

                }
            }
            // Checks that if the current new block have any existing tuples in it
            // and it's not written to the relation yet
            // If so then write it to the relation
            // If not then do nothing
            if (newBlock.getNumTuples() != 0) {
                rsWriter.writeBlock(newBlock);
                numIO++;
            }
        }
//        System.out.println("****");
//        relRS.printRelation(true, true);

        return numIO;
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