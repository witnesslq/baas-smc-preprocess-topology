package com.ai.baas.smc.preprocess.topology.core.constant;

public final class SmcHbaseConstants {
    private SmcHbaseConstants() {
    }

    public final static String BATCH_NO_TENANT_ID = "batch_no:tenant_id";

    public final static String CHARSET_UTF8 = "utf-8";

    public final static String CHARSET_GBK = "gbk";

    public static final String FIELD_SPLIT = new String(new char[] { (char) 1 });

    public static final String CVSFILE_FEILD_SPLIT = ",";

    public static final String BATCH_NO = "batch_no";

    public static final String ORDER_ID = "order_id";

    public static final String APPLY_TIME = "apply_time";

    public static final String TOTAL_RECORD = "total_record";

    public static final String CHECK_BOLT = "check_bolt";

    public static final String STATISTICS_BOLT = "statistics_bolt";

    public static final String KAFKA_SPOUT_NAME = "kafka-spout";

    /**
     * 基础元素表<br>
     * Date: 2016年3月17日 <br>
     * Copyright (c) 2016 asiainfo.com <br>
     * 
     * @author mayt
     */
    public static class TableName {
        /**
         * 流水数据表
         */
        public static final String STL_ORDER_DATA_ = "stl_order_data_";

    }

    /**
     * 列族<br>
     * Date: 2016年4月14日 <br>
     * Copyright (c) 2016 asiainfo.com <br>
     * 
     * @author mayt
     */
    public static class FamilyName {
        /**
         * 默认列族名
         */
        public static final String COLUMN_DEF = "col_def";
    }

    public static class StlOrderData {

        public static final String TENANT_ID = "tenant_id";

        public static final String BATCH_NO = "batch_no";

        public static final String OBJECT_ID = "object_id";

        public static final String ORDER_ID = "order_id";

        public static final String APPLY_TIME = "apply_time";

        public static final String VERIFY_STATE = "verify_state";

        public static final String VERIFY_DESC = "verify_desc";

    }

    /**
     * 账单格式定义<br>
     * Date: 2016年3月17日 <br>
     * Copyright (c) 2016 asiainfo.com <br>
     * 
     * @author mayt
     */
    public static class StlBillStyle {
        public static class State {
            /**
             * 正常
             */
            public static final String NORMAL = "1";

            /**
             * 注销
             */
            public static final String CANCELLED = "0";
        }
    }

    public static final class NameSpace {

        private NameSpace() {
        }

        /**
         * sys_param
         */
        public static final String SYS_PARAM_CACHE = "com.ai.baas.smc.cache.sysparam";

        public static final String POLICY_CACHE = "com.ai.baas.smc.cache.policy";

        public static final String BILL_STYLE_CACHE = "com.ai.baas.smc.cache.billstyle";

        public static final String ELEMENT_CACHE = "com.ai.baas.smc.cache.element";

        public static final String OBJECT_ELEMENT_CACHE = "com.ai.baas.smc.cache.ObjectToElementCache";

        public static final String OBJECT_POLICY_ELEMENT_CACHE = "com.ai.baas.smc.cache.ObjectToPolicyToElementCache";

        public static final String SUCCESS_RECORD = "success_record";

        public static final String FAILED_RECORD = "failed_record";

        public static final String STL_OBJ_STAT = "stl_obj_stat";

        public static final String STATS_TIMES = "stats_times";

        public static final String STATS_TIMES_COUNT = "stats_times_count";

    }

    public static class StlPolicyItemPlan {
        /**
         * 策略类型<br>
         * Date: 2016年3月17日 <br>
         * Copyright (c) 2016 asiainfo.com <br>
         * 
         * @author mayt
         */
        public static class PlanType {
            /**
             * 标准
             */
            public static final String NORMAL = "normal";

            /**
             * 阶梯
             */
            public static final String STEP = "step";

            /**
             * 分档
             */
            public static final String GRADING = "switch";
        }

        public static class CalType {
            /**
             * 按比例
             */
            public static final String RATIO = "ratio";

            /**
             * 按固定金额
             */
            public static final String FIXED = "fixed";

            /**
             * 单价
             */
            public static final String PRICE = "price";
        }
    }

    public static class StlPolicy {
        /**
         * 执行周期枚举值<br>
         * Date: 2016年3月17日 <br>
         * Copyright (c) 2016 asiainfo.com <br>
         * 
         * @author mayt
         */
        public static class ExecCycle {
            /**
             * 实时
             */
            public static final String REALTIME = "realtime";

            /**
             * 天
             */
            public static final String DAY = "day";

            /**
             * 周
             */
            public static final String WEEK = "week";

            /**
             * 月
             */
            public static final String MONTH = "month";

            /**
             * 年
             */
            public static final String YEAR = "year";
        }

        /**
         * 政策对应业务数据 <br>
         * Date: 2016年3月22日 <br>
         * Copyright (c) 2016 asiainfo.com <br>
         * 
         * @author mayt
         */
        public static class DataObjectId {
            /**
             * 客户
             */
            public static final String CUST = "cust";

            /**
             * 订购
             */
            public static final String SUBS = "subs";

            /**
             * 使用流水
             */
            public static final String ORDER = "order";
        }

        /**
         * 对账标识 Date: 2016年3月22日 <br>
         * Copyright (c) 2016 asiainfo.com <br>
         * 
         * @author mayt
         */
        public static class CheckFeeFlag {
            /**
             * 是
             */
            public static final String YES = "1";

            /**
             * 否
             */
            public static final String NO = "0";
        }

        /**
         * 政策状态<br>
         * Date: 2016年3月17日 <br>
         * Copyright (c) 2016 asiainfo.com <br>
         * 
         * @author mayt
         */
        public static class State {
            /**
             * 正常
             */
            public static final String NORMAL = "1";

            /**
             * 注销
             */
            public static final String CANCELLED = "0";
        }
    }

    public static class StlSysParam {
        public static class State {

            /**
             * 正常
             */
            public static final String NORMAL = "1";

            /**
             * 失效
             */
            public static final String INVALID = "0";
        }
    }

    public static class StlImportLog {
        public static class DataType {
            /**
             * 业务流水
             */
            public static final String ORDER = "order";

            /**
             * 账单
             */
            public static final String BILL = "bill";
        }

        public static class State {
            /**
             * 已上传
             */
            public static final String uploaded = "0";

            /**
             * 导入处理中
             */
            public static final String IMPORT_PROCESSING = "1";

            /**
             * 导入完成
             */
            public static final String IMPORT_SUCCESS = "2";

            /**
             * 数据处理中
             */
            public static final String DATA_PROCESSING = "3";

            /**
             * 数据处理完成
             */
            public static final String DATA_SUCCESS = "4";

            /**
             * 异常
             */
            public static final String EXCEPTION = "9";
        }

        public static class StateDesc {
            public static final String uploaded = "已上传";

            public static final String IMPORT_PROCESSING = "导入处理中";

            public static final String IMPORT_SUCCESS = "导入完成";

            public static final String DATA_PROCESSING = "数据处理中";

            public static final String DATA_SUCCESS = "数据处理完成";

            public static final String EXCEPTION = "异常";
        }
    }

    public static class StlBillData {
        public static class BillFrom {
            /**
             * 系统生成
             */
            public static final String SYS = "sys";

            /**
             * 第三方导入
             */
            public static final String IMPORT = "3pl";
        }

        public static class CheckState {
            /**
             * 一致
             */
            public static final String UNANIMOUS = "3";

            /**
             * 不一致
             */
            public static final String INCONFORMITY = "4";
        }

        public static class CheckStateDesc {
            /**
             * 账单一致
             */
            public static final String BILL_UNANIMOUS = "账单一致";

            /**
             * 有差异
             */
            public static final String HAS_DIFFERENCE = "有差异";
        }
    }

    public static class StlBillItemData {
        public static class ItemType {
            /**
             * 正常科目
             */
            public static final String NORMAL = "1";

            /**
             * 调账科目
             */
            public static final String ADJUST = "2";
        }

        public static class CheckState {
            /**
             * 一致
             */
            public static final String UNANIMOUS = "1";

            /**
             * 不一致
             */
            public static final String INCONFORMITY = "2";
        }

        public static class CheckStateDesc {
            /**
             * 一致
             */
            public static final String UNANIMOUS = "一致";

            /**
             * 不一致
             */
            public static final String INCONFORMITY = "不一致";
        }

        public static class ColumnName {
            /**
             * 默认列族名
             */
            public static final String COLUMN_DEF = "col_def";

        }
    }
}
