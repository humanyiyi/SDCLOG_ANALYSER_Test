package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by root on 2016/7/25.
 */
public class DefinedGroupSort extends WritableComparator {
    private static final Logger logger = Logger.getLogger(CombinationKey.class);

    public DefinedGroupSort() {
        super(CombinationKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        logger.debug("-------进入 自定义分组 flag-------");
        CombinationKey ck1 = (CombinationKey) a;
        CombinationKey ck2 = (CombinationKey) b;
        logger.debug("-------自定义分组key:" + ck1.getFirstKey().
                compareTo(ck2.getFirstKey()) + "-------");
        logger.debug("-------瑞出自定义分组 flag-------");
        return ck1.getFirstKey().compareTo(ck2.getFirstKey());
    }
}
