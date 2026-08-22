
-- 交易域核心业务数据(订单表、订单明细表、订单状态履历表、支付表、购物车表、退款表、优惠券领用表)
CREATE DATABASE IF NOT EXISTS `gmall_business` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
use `gmall_business`;

-- 订单表

-- ============ 1. 订单主表 ============
CREATE TABLE `order_info` (
  `id`                BIGINT         NOT NULL AUTO_INCREMENT COMMENT '订单编号(主键,不可变)',
  `consignee`         VARCHAR(100)   DEFAULT NULL            COMMENT '收货人姓名(下单快照)',
  `consignee_tel`     VARCHAR(20)    DEFAULT NULL            COMMENT '收货人手机号(下单快照)',
  `total_amount`      DECIMAL(18,2)  DEFAULT NULL            COMMENT '订单总金额(含优惠后实付)',
  `order_status`      SMALLINT       DEFAULT NULL            COMMENT '订单状态,见base_dic:1001未支付/1002支付中/1003已支付/1004已取消/1005已完成/1006退款中/1007已退款',
  `user_id`           BIGINT         DEFAULT NULL            COMMENT '用户ID(关联base.user_info.id)',
  `payment_way`       TINYINT        DEFAULT NULL            COMMENT '支付方式,见base_dic:1微信/2支付宝/3银联/4货到付款',
  `delivery_address`  VARCHAR(200)   DEFAULT NULL            COMMENT '收货地址(下单快照)',
  `order_comment`     VARCHAR(200)   DEFAULT NULL            COMMENT '订单备注',
  `out_trade_no`      VARCHAR(50)    DEFAULT NULL            COMMENT '对外交易编号(业务唯一,防幂等)',
  `trade_body`        VARCHAR(200)   DEFAULT NULL            COMMENT '交易主体(订单描述)',
  `create_time`       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '下单时间',
  `operate_time`      DATETIME(3)    DEFAULT NULL            COMMENT '状态变更时间(每次order_status变化必须刷新,CDC增量依据)',
  `receive_time`      DATETIME(3)    DEFAULT NULL            COMMENT '确认收货时间',
  `expire_time`       DATETIME(3)    DEFAULT NULL            COMMENT '订单过期时间(未支付超时)',
  `province_id`       INT            DEFAULT NULL            COMMENT '省份ID(收货地址所在省,关联base.base_region.id)',
  `coupon_reduce_amount`   DECIMAL(18,2) DEFAULT 0.00        COMMENT '优惠券抵扣金额(多券合计)',
  `original_total_amount`  DECIMAL(18,2) DEFAULT NULL        COMMENT '订单原始总金额(优惠前)',
  `update_time`       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间(任何列变更刷新,CDC增量依据)',
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_create_time` (`create_time`),
  KEY `idx_operate_time` (`operate_time`),
  UNIQUE KEY `uk_out_trade_no` (`out_trade_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单主表(源)';

-- ============ 2. 订单状态履历表 ============
-- (状态机/漏斗分析的事件源,订单每变更一次状态插一行,天然配合CDC增量)
CREATE TABLE `order_status_log` (
  `id`           BIGINT    NOT NULL AUTO_INCREMENT COMMENT '履历主键',
  `order_id`     BIGINT    NOT NULL                COMMENT '订单id(关联order_info.id)',
  `order_status` SMALLINT  NOT NULL                COMMENT '订单状态,见base_dic',
  `create_time`  DATETIME(3) NOT NULL              COMMENT '进入该状态的时间(毫秒,CDC增量/漏斗用)',
  PRIMARY KEY (`id`),
  KEY `idx_order_status_log` (`order_id`, `order_status`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单状态履历表(源)';

-- ============ 3. 订单明细表 ============
CREATE TABLE `order_detail` (
  `id`                   BIGINT       NOT NULL AUTO_INCREMENT COMMENT '明细编号(物理主键)',
  `order_id`             BIGINT       NOT NULL                COMMENT '订单编号(关联order_info.id)',
  `order_line_no`        INT          NOT NULL                COMMENT '订单内明细行号(同一订单内唯一,ODS去重依据)',
  `sku_id`               BIGINT       NOT NULL                COMMENT '商品SKU_ID(关联base.sku_info.id)',
  `sku_name`             VARCHAR(200) DEFAULT NULL            COMMENT '商品SKU名称(下单快照)',
  `img_url`              VARCHAR(200) DEFAULT NULL            COMMENT '商品图片URL(下单快照)',
  `order_price`          DECIMAL(18,2) DEFAULT NULL           COMMENT '商品单价(购买时价格)',
  `sku_num`              INT          DEFAULT NULL            COMMENT '购买数量',
  `create_time`          DATETIME(3)  NOT NULL                COMMENT '下单时间',
  `source_type`          SMALLINT     DEFAULT NULL            COMMENT '来源类型,见base_dic:2401用户下单/2402促销/2403购物车',
  `source_id`            BIGINT       DEFAULT NULL            COMMENT '来源ID(购物车ID等)',
  `split_activity_amount` DECIMAL(18,4) DEFAULT 0.00          COMMENT '分摊活动优惠金额(4位精度含中间值)',
  `coupon_id`            BIGINT       DEFAULT NULL            COMMENT '优惠券ID(该明细split_coupon_amount由哪张券分摊,关联business.coupon_use.id)',
  `split_coupon_amount`   DECIMAL(18,4) DEFAULT 0.00          COMMENT '分摊优惠券金额(4位精度含中间值)',
  `split_total_amount`    DECIMAL(18,4) DEFAULT NULL          COMMENT '分摊后实际金额(4位精度,汇总截断到订单总额)',
  `update_time`       DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间(CDC增量依据)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_line` (`order_id`, `order_line_no`),
  KEY `idx_sku_id` (`sku_id`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细表(源)';

-- ============ 4. 购物车表 ============
CREATE TABLE `cart_info` (
  `id`           BIGINT        NOT NULL AUTO_INCREMENT COMMENT '购物车主键',
  `user_id`      BIGINT        NOT NULL                COMMENT '用户ID',
  `sku_id`       BIGINT        NOT NULL                COMMENT '商品SKU_ID',
  `sku_name`     VARCHAR(200)  DEFAULT NULL            COMMENT '商品SKU名称(冗余)',
  `category_id`  BIGINT        DEFAULT NULL            COMMENT '三级分类ID(冗余)',
  `cart_price`   DECIMAL(18,2) DEFAULT NULL            COMMENT '加入购物车时商品单价',
  `sku_num`      INT           DEFAULT 1               COMMENT '购物车中商品数量',
  `img_url`      VARCHAR(200)  DEFAULT NULL            COMMENT '商品图片URL',
  `sku_attr`     VARCHAR(500)  DEFAULT NULL            COMMENT '商品规格JSON快照(加购时写死不可变)',
  `order_id`     BIGINT        DEFAULT NULL            COMMENT '关联订单ID(下单后回填;非空即已下单)',
  `is_checked`   TINYINT(1)    DEFAULT 1               COMMENT '是否勾选选中:0/1(同款多行业务保持一致)',
  `create_time`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '加入购物车时间',
  `operate_time` DATETIME(3)   DEFAULT NULL            COMMENT '最后操作时间(数量/勾选变更刷新)',
  `update_time`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间(CDC增量依据)',
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_sku_id` (`sku_id`),
  KEY `idx_order_id` (`order_id`),
  KEY `idx_create_time` (`create_time`)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='购物车表(源)';

-- ============ 5. 支付表 ============
CREATE TABLE `payment_info` (
  `id`               BIGINT        NOT NULL AUTO_INCREMENT COMMENT '支付主键',
  `out_trade_no`     VARCHAR(50)   NOT NULL                COMMENT '对外交易编号(业务唯一)',
  `order_id`         BIGINT        NOT NULL                COMMENT '订单编号(一单一支付,业务唯一)',
  `user_id`          BIGINT        DEFAULT NULL            COMMENT '用户ID',
  `payment_type`     TINYINT       DEFAULT NULL            COMMENT '支付类型,见base_dic:1微信/2支付宝/3银联/4货到付款',
  `trade_no`         VARCHAR(50)   DEFAULT NULL            COMMENT '第三方支付流水号',
  `total_amount`     DECIMAL(18,2) DEFAULT NULL            COMMENT '实际支付金额',
  `payment_status`   SMALLINT      DEFAULT NULL            COMMENT '支付状态,见base_dic:1001未支付/1002已支付/1003已取消(退款状态在refund_info)',
  `create_time`      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '发起支付时间',
  `callback_time`    DATETIME(3)   DEFAULT NULL            COMMENT '回调时间(第三方确认支付成功)',
  `callback_content` TEXT          DEFAULT NULL            COMMENT '回调内容JSON(对账/审计;实时消费勿SELECT整列)',
  `update_time`      DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间(CDC增量依据)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_id` (`order_id`),
  UNIQUE KEY `uk_out_trade_no` (`out_trade_no`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='支付表(源)';

-- ============ 6. 退款表 ============
CREATE TABLE `refund_info` (
  `id`                 BIGINT        NOT NULL AUTO_INCREMENT COMMENT '退款主键',
  `user_id`            BIGINT        NOT NULL                COMMENT '用户ID',
  `order_id`           BIGINT        NOT NULL                COMMENT '订单编号',
  `order_detail_id`    BIGINT        NOT NULL                COMMENT '订单明细编号(一条明细至多一次退款,业务唯一)',
  `sku_name`           VARCHAR(200)  DEFAULT NULL            COMMENT '商品SKU名称(下单快照)',
  `refund_amount`      DECIMAL(18,2) NOT NULL                COMMENT '退款金额',
  `refund_num`         INT           DEFAULT 1               COMMENT '退款数量',
  `refund_status`      SMALLINT      DEFAULT NULL            COMMENT '退款状态,见base_dic:0701申请/0702退款中/0703已退款/0704拒绝',
  `refund_type`        TINYINT       DEFAULT NULL            COMMENT '退款类型,见base_dic:1仅退款/2退货退款/3换货',
  `refund_reason`      VARCHAR(200)  DEFAULT NULL            COMMENT '退款原因',
  `refund_reason_type` TINYINT       DEFAULT NULL            COMMENT '退款原因类型,见base_dic',
  `create_time`        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '申请退款时间',
  `refund_time`        DATETIME(3)   DEFAULT NULL            COMMENT '退款完成/到账时间(退款事实时间)',
  `operate_time`       DATETIME(3)   DEFAULT NULL            COMMENT '状态最后变更时间(每次refund_status变更刷新)',
  `update_time`        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间(CDC增量依据)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_detail` (`order_detail_id`),
  KEY `idx_order_id` (`order_id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='退款表(源)';

-- ============ 7. 优惠券领用核销表 ============
CREATE TABLE `coupon_use` (
  `id`              BIGINT        NOT NULL AUTO_INCREMENT COMMENT '领用编号(主键,券实例)',
  `coupon_id`       BIGINT        NOT NULL                COMMENT '优惠券模板ID(关联base.coupon_info.id)',
  `coupon_type`     TINYINT       DEFAULT NULL            COMMENT '券类型快照(下单时写入,见base_dic:1满减/2无门槛)',
  `user_id`         BIGINT        NOT NULL                COMMENT '用户ID',
  `order_id`        BIGINT        DEFAULT NULL            COMMENT '订单ID(用券下单回填)',
  `coupon_status`   SMALLINT      DEFAULT NULL            COMMENT '券状态,见base_dic:1401未使用/1404已锁定/1402已核销/1403已过期',
  `coupon_reduce_amount` DECIMAL(18,2) DEFAULT NULL       COMMENT '该券实际抵扣金额(下单时按券规则+商品价算好写死,历史不变)',
  `get_time`        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '领取时间',
  `lock_time`       DATETIME(3)   DEFAULT NULL            COMMENT '锁定时间(下单占用)',
  `using_time`      DATETIME(3)   DEFAULT NULL            COMMENT '下单用券时间',
  `used_time`       DATETIME(3)   DEFAULT NULL            COMMENT '核销时间(订单完成)',
  `expire_time`     DATETIME(3)   DEFAULT NULL            COMMENT '过期时间',
  `update_time`     DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '行变更时间(CDC增量依据)',
  PRIMARY KEY (`id`),
  KEY `idx_coupon_id` (`coupon_id`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_order_id` (`order_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券领用核销表(源)';

