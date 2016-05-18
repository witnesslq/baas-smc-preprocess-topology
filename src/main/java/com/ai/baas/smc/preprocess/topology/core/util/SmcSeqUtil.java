package com.ai.baas.smc.preprocess.topology.core.util;

import java.security.SecureRandom;

import com.ai.baas.smc.preprocess.topology.core.constant.SmcSeqConstants;
import com.ai.opt.sdk.components.sequence.util.SeqUtil;
import com.ai.opt.sdk.util.UUIDUtil;

public final class SmcSeqUtil {

    private SmcSeqUtil() {
    }

    private static String getSixRandom() {

        int seq = new SecureRandom().nextInt(1000000);
        String seqStr = String.valueOf(seq);
        if (seqStr.length() < 6) {
            seqStr = "000000" + seqStr;
        }
        return seqStr.substring(seqStr.length() - 6);
    }

    private static String getFourRandom() {

        int seq = new SecureRandom().nextInt(10000);
        String seqStr = String.valueOf(seq);
        if (seqStr.length() < 4) {
            seqStr = "0000" + seqStr;
        }
        return seqStr.substring(seqStr.length() - 4);
    }

    /**
     * 政策ID
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createPolicyId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_POLICY$POLICY_ID$SEQ);
    }

    public static Long createElementId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_ELEMENT$ELEMENT_ID$SEQ);
    }

    public static Long createElementAttrId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_ELEMENTATTR$ELEMENTATTR_ID$SEQ);
    }

    /**
     * 政策项ID
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createItemId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_POLICY_ITEM$ITEM_ID$SEQ);
    }

    /**
     * 政策适配对象主键
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createConditionId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_POLICY_ITEM_CONDITION$CONDITION_ID$SEQ);
    }

    /**
     * 政策结算策略ID
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createPlanId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_POLICY_ITEM_PLAN$PLAN_ID$SEQ);
    }

    /**
     * 账单样式ID
     * 
     * @return
     * @author wangjl9
     * @ApiDocMethod
     */
    public static Long createBillStyleId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_BILL_STYLE$BILL_STYLE_ID$SEQ);
    }

    /**
     * 详单项ID
     * 
     * @return
     * @author wangjl9
     * @ApiDocMethod
     */
    public static Long createBillDetailStyleItemId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_BILL_DETAIL_STYLE_ITEM$ITEM_ID$SEQ);
    }

    /**
     * 
     * 账单项ID
     * 
     * @return
     * @author wangjl9
     * @ApiDocMethod
     */
    public static Long createBillStyleItemId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_BILL_STYLE_ITEM$ITEM_ID$SEQ);
    }

    /**
     * 
     * 业务参数配置表流水主键
     * 
     * @return
     * @author wangjl9
     * @ApiDocMethod
     */
    public static String createGuidkey() {
        return UUIDUtil.genId32();
    }

    /**
     * 数据导入日志ID
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createLogId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_IMPORT_LOG$LOG_ID$SEQ);
    }

    /**
     * 账单ID
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createBillId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_BILL_DATA$BILL_ID$SEQ);
    }

    /**
     * 账单项ID
     * 
     * @return
     * @author mayt
     * @ApiDocMethod
     */
    public static Long createBillItemId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_BILL_ITEM_DATA$BILL_ITEM_ID$SEQ);
    }

    /**
     * 结算对象统计数据表,主键ID(内部)
     * 
     * @return
     * @author wangjl9
     * @ApiDocMethod
     */
    public static Long createDataId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_OBJ_STAT$DATA_ID$SEQ);
    }

    public static Long createStreamId() {
        return SeqUtil.getNewId(SmcSeqConstants.STL_ORDER_DATA$STREAM_ID$SEQ);
    }
}
