-- ============================================================
-- gmall 实时数仓 - 数据字典表 base_dic
-- 使用前先执行此文件, 生成固定的 52 条字典数据
-- ============================================================

USE `gmall_base`;

DROP TABLE IF EXISTS `base_dic`;
CREATE TABLE `base_dic` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '字典主键',
  `dic_type`    VARCHAR(60)  NOT NULL               COMMENT '字典类型(建议以域前缀区分:BUSINESS_/BASE_/LOG_)',
  `code`        VARCHAR(20)  NOT NULL               COMMENT '字典编码',
  `name`        VARCHAR(50)  NOT NULL               COMMENT '字典名称',
  `sort`        INT          DEFAULT 0              COMMENT '排序(同类型内排序)',
  `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_type_code` (`dic_type`, `code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='全局数据字典表(方案A,三库共用,dic_type前缀区分域)';

INSERT INTO `base_dic` (`dic_type`, `code`, `name`, `sort`) VALUES


('BUSINESS_order_status', '1001', '未支付',  1),-- BUSINESS 业务域(交易)
('BUSINESS_order_status', '1002', '支付中',  2),
('BUSINESS_order_status', '1003', '已支付',  3),
('BUSINESS_order_status', '1004', '已取消',  4),
('BUSINESS_order_status', '1005', '已完成',  5),
('BUSINESS_order_status', '1006', '退款中',  6),
('BUSINESS_order_status', '1007', '已退款',  7),

('BUSINESS_payment_way', '1', '微信',   1),
('BUSINESS_payment_way', '2', '支付宝', 2),
('BUSINESS_payment_way', '3', '银联',   3),
('BUSINESS_payment_way', '4', '货到付款', 4),

('BUSINESS_source_type', '2401', '用户下单', 1),
('BUSINESS_source_type', '2402', '促销活动', 2),
('BUSINESS_source_type', '2403', '购物车',   3),

('BUSINESS_payment_status', '1001', '未支付', 1),
('BUSINESS_payment_status', '1002', '已支付', 2),
('BUSINESS_payment_status', '1003', '已取消', 3),

('BUSINESS_refund_status', '0701', '申请退款', 1),
('BUSINESS_refund_status', '0702', '退款中',   2),
('BUSINESS_refund_status', '0703', '已退款',   3),
('BUSINESS_refund_status', '0704', '退款拒绝', 4),

('BUSINESS_refund_type', '1', '仅退款',   1),
('BUSINESS_refund_type', '2', '退货退款', 2),
('BUSINESS_refund_type', '3', '换货',     3),

('BUSINESS_refund_reason_type', '1', '质量问题',     1),
('BUSINESS_refund_reason_type', '2', '与描述不符',   2),
('BUSINESS_refund_reason_type', '3', '七天无理由',   3),
('BUSINESS_refund_reason_type', '4', '未收到货',     4),
('BUSINESS_refund_reason_type', '5', '其他',         5),

('BUSINESS_coupon_status', '1401', '未使用', 1),
('BUSINESS_coupon_status', '1404', '已锁定', 2),
('BUSINESS_coupon_status', '1402', '已核销', 3),
('BUSINESS_coupon_status', '1403', '已过期', 4),


('BASE_user_level', '1', '普通会员', 1),-- BASE 基础域(用户/商品/通用)
('BASE_user_level', '2', '青铜',     2),
('BASE_user_level', '3', '白银',     3),
('BASE_user_level', '4', '黄金',     4),
('BASE_user_level', '5', '铂金',     5),
('BASE_user_level', '6', '钻石',     6),

('BASE_user_status', '1001', '正常', 1),
('BASE_user_status', '1002', '冻结', 2),
('BASE_user_status', '1003', '注销', 3),

('BASE_gender', '0', '未知', 0),
('BASE_gender', '1', '男',   1),
('BASE_gender', '2', '女',   2),

('BASE_coupon_type', '1', '满减券',   1),
('BASE_coupon_type', '2', '无门槛券', 2),


('LOG_event_type', '1001', '启动', 1),-- LOG 日志域(埋点)
('LOG_event_type', '1002', '页面浏览', 2),
('LOG_event_type', '1003', '动作',   3),
('LOG_event_type', '1004', '曝光',   4),
('LOG_event_type', '1005', '错误',   5);


