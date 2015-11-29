package simpledb.buffer;

import java.util.HashMap;

import simpledb.file.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private int numAvailable;
   private HashMap<Block, Buffer> bufferPoolMap; 
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferPoolMap = new HashMap<Block,Buffer>();
      numAvailable = numbuffs;
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
	   for (Block block : bufferPoolMap.keySet()) {
		   if (bufferPoolMap.get(block).isModifiedBy(txnum)) {
			   bufferPoolMap.get(block).flush();
			}
	   }
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         /**
          * Add the buff to bufferPoolMap
          */
         bufferPoolMap.put(blk, buff);
      }
      if (!buff.isPinned())
         numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      /**
       * Add the buff to bufferPoolMap
       */
      bufferPoolMap.put(buff.block(), buff);
      numAvailable--;
      buff.pin();
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
      return bufferPoolMap.get(blk);    
   }
   
   private Buffer chooseUnpinnedBuffer() {
	   Buffer buff = null;
	   if(numAvailable > 0) {
		   buff = new Buffer();
	   }
	   else {
		   buff = findLeastRecentlyModified();
	   }
	   return buff;
   }

   /**
    * Finds a buffer which was least recently modified and is unpinned
    * If no such buffer found, tries to find an unpinned buffer which 
    * was not modified and has the least LSN. 
    * Returns null if could not find any such buffer
    * 
    * @return
    */
    private Buffer findLeastRecentlyModified() {
    	Buffer buffModified = null;
    	Buffer buffUnModified = null;
    	int minLSNModified = Integer.MAX_VALUE;
    	int minLSNUnModified = Integer.MAX_VALUE;
    	
    	for (Block block : bufferPoolMap.keySet()) {
    		System.out.println("Block number is " + block.number() +"\t" + block.fileName());
    		Buffer buff = bufferPoolMap.get(block);
    		/**
    		 * Loop through unpinned buffer
    		 */
    		if (!buff.isPinned()) {
    			int lsn = buff.getLSN();
    			/*
    			 * buffer is modified
    			 */
    			if (lsn >= 0 && buff.isModified()) {
    				if (lsn < minLSNModified) {
    					buffModified = buff;
    					minLSNModified = lsn;
    				}
    			}
    			/*
    			 * buffer is not modified
    			 */
    			else {
    				if (lsn < minLSNUnModified) {
    					buffUnModified = buff;
    					minLSNUnModified = lsn;
    				}
    			}
    		}
    	}    	
    	if (buffModified != null) {
    		bufferPoolMap.remove(buffModified.block());
    		return buffModified;    		
    	}
    	else if (buffUnModified != null) {
    		bufferPoolMap.remove(buffUnModified.block());
    		return buffUnModified;
    	}    	
    	return null;
    }
    
    /**
     * Stores the readCount and writeCount of a particular buffer (which is given as argument)
     * Still not clear where this method should get called from
     * Does it get called for all available buffers?
     */
    public void getStatistics(Buffer buff){
    	int i=1;
    	for (Block block : bufferPoolMap.keySet()) 
    	{
    		Buffer buff_temp = bufferPoolMap.get(block);
    		/**
    		 * Loop through all pinned and un-pinned buffer
    		 */
    
    				/**
    				 *int rCount = buff_temp.getReadCount();
    				 *int wCount = buff_temp.getWriteCount();
    				**/
    				System.out.println("------Buffer " + i + " ------");
    				System.out.println("Read Count -->" + buff_temp.getReadCount());
    				System.out.println("Write Count -->" + buff_temp.getWriteCount());
    				System.out.println("Pin Count -->" + buff_temp.getPinCount());
    				System.out.println("Un pin Count -->" + buff_temp.getUnpinCount());
    				System.out.println("Block Count -->" + buff_temp.getBlockCount());
    				i++;   				
    				
    		
    	}
    	
    }
}
