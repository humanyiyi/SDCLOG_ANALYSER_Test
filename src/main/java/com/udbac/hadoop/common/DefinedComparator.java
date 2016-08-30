package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by root on 2016/7/25.
 */
public class DefinedComparator extends WritableComparator {
    private static final Logger logger = Logger.getLogger(CombinationKey.class);

    public DefinedComparator() {
        super(CombinationKey.class, true);
    }

    public int compare(WritableComparable combinationKeyOne,
                       WritableComparable CombinationKeyOther) {
        logger.debug("---------进入自定义排序---------");

        CombinationKey c1 = (CombinationKey) combinationKeyOne;
        CombinationKey c2 = (CombinationKey) CombinationKeyOther;

        if (!c1.getFirstKey().equals(c2.getFirstKey())) {
            logger.debug("---------退出自定义排序1---------");
            return c1.getFirstKey().compareTo(c2.getFirstKey());
        } else {
            logger.debug("---------退出自定义排序2---------");
            return Integer.compare(c1.getSecondKey(), c2.getSecondKey());
        }

    }
}
