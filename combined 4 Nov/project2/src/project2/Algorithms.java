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

        //Check if the number of block is less than the multiplication of memorySize
        if (rel.getNumBlocks() <= Setting.memorySize * Setting.memorySize) {
            Boolean flag = false;
            Block bufferOutput = new Block();
            Relation relationObj = new Relation(rel.name);
            RelationWriter finalRelationWriter = relationObj.getRelationWriter();

            ArrayList<Relation> sublistRelationList = new ArrayList<>();
            ArrayList<RelationLoader> sublistLoaderList = new ArrayList<>();
            ArrayList<Block[]> memoryBuffer = new ArrayList<>();

            /*=========================*/
            /*Phase 1: Form SubList*/
            /*=========================*/
            RelationLoader relationLoader = rel.getRelationLoader();
            //while loop to check for hasNextBlock for relationLoader
            while (relationLoader.hasNextBlock()) {
                Block[] blockArr = relationLoader.loadNextBlocks(Setting.memorySize);
                ArrayList<Tuple> tupleSubListArrList = new ArrayList<>();
                Block newBlockObj = new Block();
                int blocksCount = 0;

                //For M blocks of memory, loop through all tuples and add into sublist.
                for (Block blockObj : blockArr) {
                    if (blockObj != null) {
                        numIO++;
                        for (Tuple tupleObj : blockObj.tupleLst) {
                            if (tupleObj != null) {
                                tupleSubListArrList.add(tupleObj);
                            }
                        }
                    }
                }
                //Sort tuple sublist using collection sort
                Collections.sort(tupleSubListArrList, new ComparatorClass());

                //Reform tuple sublist into blocks 
                for (int i = 0; i < tupleSubListArrList.size(); i++) {
                    if (!newBlockObj.insertTuple(tupleSubListArrList.get(i))) {
                        blockArr[blocksCount] = newBlockObj;
                        newBlockObj = new Block();
                        newBlockObj.insertTuple(tupleSubListArrList.get(i));
                        blocksCount++;
                    }
                }
                blockArr[blocksCount] = newBlockObj;

                //Write the sorted subList to subList relation
                Relation sublistRelation = new Relation(rel.name + "_SubList");
                RelationWriter relSubListWriter = sublistRelation.getRelationWriter();

                for (Block blkObj : blockArr) {
                    if (blkObj != null) {
                        relSubListWriter.writeBlock(blkObj);
                        numIO++;
                    }
                }
                sublistRelationList.add(sublistRelation);
            }

            /*=========================*/
            /*Phase 2: Sort the subList*/
            /*=========================*/
            //Check if the size of the sublistRelation is less than Memmory size -1
            if (sublistRelationList.size() < Setting.memorySize - 1) {
                //Loop subList relation and create a relationLoader and allocate a buffer
                for (int i = 0; i < sublistRelationList.size(); i++) {
                    Relation sublistRel = sublistRelationList.get(i);
                    //RelationLoader
                    RelationLoader sublistLoader = sublistRel.getRelationLoader();
                    sublistLoaderList.add(sublistLoader);

                    //Assign the 1st block to block array, which will then add into the memoryBuffer
                    Block[] blocks = sublistLoader.loadNextBlocks(1);
                    memoryBuffer.add(blocks);
                    numIO++;
                }

                //while loop sublist read...
                while (true) {
                    ArrayList<Tuple> tupleList = new ArrayList<>();
                    //loop the memoryBuffer to get the tuples and add into the tupleList
                    for (int i = 0; i < memoryBuffer.size(); i++) {
                        for (Tuple t : memoryBuffer.get(i)[0].tupleLst) {
                            tupleList.add(t);
                        }
                    }

                    //check if tupleList is empty - flag false, else do the followings
                    if (tupleList.isEmpty()) {
                        flag = false;
                    } else {
                        //Call collections.sort to sort the tupleList
                        Collections.sort(tupleList, new ComparatorClass());

                        //get the first index(0) from the tupleList and insert into output buffer
                        //if full then write to the finalRelationWriter.
                        if (!bufferOutput.insertTuple(tupleList.get(0))) {
                            finalRelationWriter.writeBlock(bufferOutput);
                            bufferOutput = new Block();
                            bufferOutput.insertTuple(tupleList.get(0));
                        }

                        //match of removed tuples with in memory buffers and remove the tuple
                        for (int i = 0; i < memoryBuffer.size(); i++) {
                            for (Tuple t : memoryBuffer.get(i)[0].tupleLst) {
                                if (t.key == tupleList.get(0).key && t.value.equals(tupleList.get(0).value)) {
                                    memoryBuffer.get(i)[0].tupleLst.remove(t);
                                    break;
                                }
                            }
                        }

                        //if current memory buffers are empty, loop to read from relation sublist arraylist. 
                        for (int i = 0; i < sublistRelationList.size(); i++) {
                            //if number of tuples of memory buffer is 0, read sublistLoader
                            if (memoryBuffer.get(i)[0].getNumTuples() == 0) {
                                RelationLoader sublistLoader = sublistLoaderList.get(i);
                                if (sublistLoader.hasNextBlock()) {
                                    numIO++;
                                    memoryBuffer.set(i, sublistLoader.loadNextBlocks(1));
                                    flag = true;
                                    break;
                                }
                            } else {
                                flag = true;
                            }
                        }
                    }

                    //Check if there anymore before writing to the finalRelationWriter -- end.
                    if (flag == false) {
                        finalRelationWriter.writeBlock(bufferOutput);
                        bufferOutput = new Block();
                        break;
                    }
                }
                //Print relation...
//                System.out.println("\n-------Start Printing Relation:\t"+ rel.name + "--------\n");
                relationObj.printRelation(false, false);
//                System.out.println("\n-------End Printing Relation:\t"+ rel.name + "--------\n");
            } else {
                System.out.println("Number of sublist is more than M-1");
            }
        } else {
            System.out.println("***WARNING: Either B(R) or B(S) is larger than M^2");
        }
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
        int blockCountR = relR.getNumBlocks();
        int blockCountS = relS.getNumBlocks();
        
        
        relR.printRelation(false, false);
        relS.printRelation(false, false);
        //Declare memorySize and blockFactor
        int memorySize = Setting.memorySize;
        int blockFactor = Setting.blockFactor;
        
        //Check the min of number of block of relR n relS against the M^2
        if(!(Math.min(blockCountR, blockCountS)<= memorySize*memorySize)){
            System.out.println("***WARNING: Minimum number of block(B(R),B(S)) is larger than M^2");
            return 0;
        }
        //Create Arraylist of bucket R
        ArrayList bucketRArrList = new ArrayList<>();
        for (int i = 0; i < memorySize - 1; i++) {
            ArrayList<Block> bucketList = new ArrayList<>();
            bucketRArrList.add(bucketList);
        }

        //Create Arraylist of bucket S
        ArrayList bucketSArrList = new ArrayList<>();
        for (int i = 0; i < memorySize - 1; i++) {
            ArrayList<Block> bucketList = new ArrayList<>();
            bucketSArrList.add(bucketList);
        }

        //Create Arraylist of buffer R
        ArrayList<Block> bufferRArrList = new ArrayList<>();
        for (int i = 0; i < memorySize - 1; i++) {
            Block b = new Block();
            bufferRArrList.add(b);
        }

        //Create Arraylist of buffer S
        ArrayList<Block> bufferSArrList = new ArrayList<>();
        for (int i = 0; i < memorySize - 1; i++) {
            Block b = new Block();
            bufferSArrList.add(b);
        }

        //Load relR using RelationLoader() 
        RelationLoader relationLoader = relR.getRelationLoader();
        while (relationLoader.hasNextBlock()) {
            Block[] blocks = relationLoader.loadNextBlocks(memorySize);
            for (Block b : blocks) {
                if (b != null) {
                    numIO++;
                    for (Tuple t : b.tupleLst) {
                        if (t != null) {
                            //Hash the Relation using modulus memorySize-1 vlaue
                            int hashValue = t.key % (memorySize - 1);
                            Block newBlockObj = bufferRArrList.get(hashValue);

                            //Check and put the rest of the blocks in the buffer
                            if (newBlockObj.tupleLst.size() == blockFactor) {
                                numIO++;
                                ArrayList<Block> blockArrList = (ArrayList<Block>) bucketRArrList.get(hashValue);
                                blockArrList.add(newBlockObj);
                                bucketRArrList.set(hashValue, blockArrList);
                                newBlockObj = new Block();
                                newBlockObj.insertTuple(t);
                                bufferRArrList.set(hashValue, newBlockObj);
                            } else {
                                newBlockObj.insertTuple(t);
                            }
                        }
                    }
                }
            }
        }

        //Put the rest of the blocks in the buffer
        for (int i = 0; i < memorySize - 1; i++) {
            Block newBlockObj = bufferRArrList.get(i);
            if (newBlockObj.tupleLst.size() > 0) {
                ArrayList<Block> blockArrList = (ArrayList<Block>) bucketRArrList.get(i);
                blockArrList.add(newBlockObj);
                bucketRArrList.set(i, blockArrList);
                numIO++;
            }
        }

        //Load relS using RelationLoader() 
        relationLoader = relS.getRelationLoader();
        while (relationLoader.hasNextBlock()) {
            Block[] blocks = relationLoader.loadNextBlocks(memorySize);
            for (Block b : blocks) {
                if (b != null) {
                    numIO++;
                    for (Tuple t : b.tupleLst) {
                        if (t != null) {
                            //Hash the Relation using modulus memorySize-1 vlaue
                            int hashValue = t.key % (memorySize - 1);
                            Block newBlockObj = bufferSArrList.get(hashValue);

                            //Check and put the rest of the blocks in the buffer
                            if (newBlockObj.tupleLst.size() == blockFactor) {
                                numIO++;
                                ArrayList<Block> blockArrList = (ArrayList<Block>) bucketSArrList.get(hashValue);
                                blockArrList.add(newBlockObj);
                                bucketSArrList.set(hashValue, blockArrList);
                                newBlockObj = new Block();
                                newBlockObj.insertTuple(t);
                                bufferSArrList.set(hashValue, newBlockObj);
                            } else {
                                newBlockObj.insertTuple(t);
                            }
                        }
                    }
                }
            }
        }

        //Put the rest of the blocks in the disk
        for (int i = 0; i < memorySize - 1; i++) {
            Block newBlockObj = bufferSArrList.get(i);
            if (newBlockObj.tupleLst.size() > 0) {
                ArrayList<Block> blockArrList = (ArrayList<Block>) bucketSArrList.get(i);
                blockArrList.add(newBlockObj);
                bucketSArrList.set(i, blockArrList);
                numIO++;
            }
        }

        //Join relR and relS
        ArrayList<Block> blockArrListRS = new ArrayList<>();
        int blockCountRS = 0;

        //Creating M-1 buffer for relS
        ArrayList<Block> buffer = new ArrayList<>();
        for (int i = 0; i < memorySize - 1; i++) {
            Block b = new Block();
            buffer.add(b);
        }

        //Loop through each bucket
        for (int i = 0; i < memorySize - 1; i++) {
            ArrayList<Block> tempBucketS = (ArrayList<Block>) bucketSArrList.get(i);
            ArrayList<Block> tempBucketR = (ArrayList<Block>) bucketRArrList.get(i);

            //Read in M-1 buffer from bucketS
            boolean flag = true;
            int track = 0;

            //Check for bucket > buffer size
            while (flag == true) {

                /* bring m-1 number of blocks into buffer */
                for (int k = 0; k < memorySize - 1; k++) {
                    if (track > tempBucketS.size() - 1) {
                        flag = false;
                        break;
                    } else {
                        buffer.set(k, tempBucketS.get(track));
                        track++;
                        numIO++;
                    }
                }

                //Read in 1 block of relR
                for (Block r : tempBucketR) {
                    numIO++;
                    for (Tuple tr : r.tupleLst) {
                        for (Block s : buffer) {
                            for (Tuple ts : s.tupleLst) {
                                if (tr.key == ts.key) {
                                    JointTuple joint = new JointTuple(tr, ts);
                                    if (blockArrListRS.isEmpty()) {
                                        Block newBlockObj = new Block();
                                        newBlockObj.insertTuple(joint);
                                        blockArrListRS.add(newBlockObj);
                                    } else {
                                        if (blockArrListRS.get(blockCountRS).tupleLst.size() == blockFactor) {
                                            Block newBlockObj = new Block();
                                            newBlockObj.insertTuple(joint);
                                            blockArrListRS.add(newBlockObj);
                                            blockCountRS++;
                                        } else {
                                            Block newBlockObj = blockArrListRS.get(blockCountRS);
                                            newBlockObj.insertTuple(joint);
                                            blockArrListRS.set(blockCountRS, newBlockObj);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        //Use RelationWriter() to write 
        RelationWriter relationWriter = relRS.getRelationWriter();
        for (Block b : blockArrListRS) {
            if (b != null) {
                relationWriter.writeBlock(b);
            }
        }

        //Print relation...
//        System.out.println("\n-------Start Printing Relation:\t"+ relRS.name + "--------\n");  
        relRS.printRelation(false, false);
//        System.out.println("\n-------End Printing Relation:\t"+ relRS.name + "--------\n");

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
        
        int memorySizeValue = Setting.memorySize;
        
        int blockCountR = relR_T.getNumBlocks();
        int blockCountS = relR_T.getNumBlocks();
        int totalBlockCountRS = blockCountR + blockCountS;
        
        //Check the number of block against the memory size
        if(!(totalBlockCountRS <= memorySizeValue*memorySizeValue)){
            System.out.println("***WARNING: Total number of block (B(R)+B(S)) is larger than M^2");
            return 0;
        }
        
        /*===============*/
        /*RELATION R*/
        /*===============*/
        ArrayList relRArrLst = sortRelation(relR_T, numIO);
        relR = (Relation) relRArrLst.get(0);
        numIOR = numIOR + (int) relRArrLst.get(1);
        numIO = numIO + numIOR;

//        System.out.println("numIOR cost for Sorted RelR:\t" + numIOR);
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
//        System.out.println("numIOS cost for Sorted RelS:\t" + numIOS);

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
//        System.out.println("Total numIO cost for both RelR & RelS:\t" + numIO);
        //output the join of all tuples of R and S with common Y-value.
        numIO = numIO + mergeJRelationRS(relationRSubList, relationSSubList, relRS, numIO);

        //print the tuple result relRS
//        System.out.println("\n--------Start Printing Relation:\t"+ relRS.name + "--------\n");
        relRS.printRelation(false, false);
//        System.out.println("\n--------End Printing Relation:\t"+ relRS.name + "--------\n");
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
        RelationWriter relationWriterObj = paramRelRS.getRelationWriter();

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
                                    //check for common key value. if same - join else dont join
                                    if (relRTupleObj.key == relSTupleObj.key) {
                                        JointTuple joinTupleObj = new JointTuple(relRTupleObj, relSTupleObj);
                                        // Check if block is full before insertion
                                        if (!newBlockObj.insertTuple(joinTupleObj)) {
                                            //Write newBlockObj to relationWriterObj(output)
                                            relationWriterObj.writeBlock(newBlockObj);
                                            //Increment paramNumIO cost
                                            paramNumIO++;
                                            //Re-initiate newBlockObj
                                            newBlockObj = new Block();
                                            //insert joinTuple to newBlockObj
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
                relationWriterObj.writeBlock(newBlockObj);
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

        //All 3 alogirthms methods are placed here...
        System.out.println("\n******************Start of mergeSortRelation****************");
        int mergeSortIOR = algoObj.mergeSortRelation(relR);
        int mergeSortIOS = algoObj.mergeSortRelation(relS);
        System.out.println("Merge Sort I/O Cost RelR:\t" + mergeSortIOR);
        System.out.println("Merge Sort I/O Cost RelS:\t" + mergeSortIOS);
        System.out.println("******************End of mergeSortRelation******************\n");
        
        
        System.out.println("\n*****************Start of refinedSortMergeJoinRelations***************");
        // Call refine sort merge join relations method and
        // pass in relation R and S and the relation RS to store the results of join
        int refinedSMJIO = algoObj.refinedSortMergeJoinRelations(relR, relS, relRS);
        // Print out Refined Sort Merge Join IO cost
        System.out.println("Refined Sort Merge Join I/O Cost:\t" + refinedSMJIO);
        System.out.println("*****************End of refinedSortMergeJoinRelations***************\n");
        
        System.out.println("\n******************Start of hashJoin****************");
        // pass in relation R and S and the relation RS to store the results of join
        int hashJoinRS = algoObj.hashJoinRelations(relR, relS, relRS);
        System.out.println("Hash Join I/O Cost RelRS:\t" + hashJoinRS);
        System.out.println("******************End of hashJoin******************\n");        
        
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
