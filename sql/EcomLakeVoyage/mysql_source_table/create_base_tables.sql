
CREATE DATABASE IF NOT EXISTS `gmall_base` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE `gmall_base`;

-- ============ 1. 优惠券模板表 ============
CREATE TABLE `coupon_info` (
  `id`              BIGINT        NOT NULL AUTO_INCREMENT COMMENT '优惠券ID(券模板)',
  `coupon_type`     TINYINT       NOT NULL                COMMENT '券类型,见base_dic:1满减券/2无门槛券',
  `full_amount`     DECIMAL(18,2) DEFAULT NULL            COMMENT '满减券门槛金额(满X元可用);无门槛为NULL',
  `reduce_amount`   DECIMAL(18,2) DEFAULT NULL            COMMENT '立减金额(满减减Y/无门槛直接减Y)',
  `coupon_name`     VARCHAR(100)  DEFAULT NULL            COMMENT '券名称',
  `use_condition`   VARCHAR(500)  DEFAULT NULL            COMMENT '使用条件说明',
  `create_time`     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间(维度变更信号)',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券模板表(源/维)';

-- ============ 2. 品牌表 ============
CREATE TABLE `base_brand` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '品牌ID(主键)',
  `brand_name`  VARCHAR(100) NOT NULL                COMMENT '品牌名称',
  `logo_url`    VARCHAR(200) DEFAULT NULL            COMMENT '品牌LOGO',
  `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间(维度变更信号)',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品牌表(源/维)';

-- ============ 3. 分类表(三级固定) ============
CREATE TABLE `base_category` (
  `id`            BIGINT      NOT NULL AUTO_INCREMENT COMMENT '分类ID(主键)',
  `category_name` VARCHAR(100) NOT NULL               COMMENT '分类名称',
  `level`         TINYINT     NOT NULL                COMMENT '层级:1一级/2二级/3三级',
  `parent_id`     BIGINT      DEFAULT NULL            COMMENT '父分类ID(一级为空)',
  `create_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`   DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间(维度变更信号)',
  PRIMARY KEY (`id`),
  KEY `idx_parent_id` (`parent_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分类表(源/维,一级/二级/三级)';

-- ============ 4. 地区表(省/市/区 + 大区) ============
CREATE TABLE `base_region` (
  `id`          BIGINT       NOT NULL AUTO_INCREMENT COMMENT '地区ID(主键)',
  `region_code` VARCHAR(20)  DEFAULT NULL            COMMENT '行政区划代码',
  `region_name` VARCHAR(50)  NOT NULL                COMMENT '地区名称',
  `level`       TINYINT      NOT NULL                COMMENT '层级:1省/2市/3区',
  `parent_id`   BIGINT       DEFAULT NULL            COMMENT '父地区ID(省为空)',
  `big_region`  VARCHAR(20)  DEFAULT NULL            COMMENT '大区:华东/华北/华南/华中/西南/西北/东北',
  `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_parent_id` (`parent_id`),
  KEY `idx_big_region` (`big_region`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='地区表(源/维,省市区+大区)';

-- ============ 5. SPU表 ============
CREATE TABLE `spu_info` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SPU_ID(主键)',
  `spu_name`     VARCHAR(200) NOT NULL                COMMENT 'SPU名称(商品名称)',
  `description`  VARCHAR(500) DEFAULT NULL            COMMENT '商品描述',
  `category3_id` BIGINT       NOT NULL                COMMENT '三级分类ID(关联base_category.id)',
  `brand_id`     BIGINT       NOT NULL                COMMENT '品牌ID(关联base_brand.id)',
  `create_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间(维度变更信号)',
  PRIMARY KEY (`id`),
  KEY `idx_category3_id` (`category3_id`),
  KEY `idx_brand_id` (`brand_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SPU表(源/维)';

-- ============ 6. SKU表 ============
CREATE TABLE `sku_info` (
  `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT 'SKU_ID(主键)',
  `sku_name`      VARCHAR(200) NOT NULL                COMMENT 'SKU名称(规格名称)',
  `spu_id`        BIGINT       NOT NULL                COMMENT 'SPU_ID(关联spu_info.id)',
  `category3_id`  BIGINT       NOT NULL                COMMENT '三级分类ID(冗余,关联base_category.id)',
  `brand_id`      BIGINT       NOT NULL                COMMENT '品牌ID(冗余,关联base_brand.id)',
  `price`         DECIMAL(18,2) DEFAULT NULL           COMMENT '商品价格',
  `weight`        DECIMAL(10,2) DEFAULT NULL           COMMENT '商品重量(kg)',
  `img_url`       VARCHAR(200)  DEFAULT NULL           COMMENT '商品图片URL',
  `is_sale`       TINYINT(1)    DEFAULT 0              COMMENT '是否上架:0下架/1上架',
  `sku_attr`      VARCHAR(500)  DEFAULT NULL           COMMENT 'SKU规格属性JSON(当前态)',
  `create_time`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间(维度变更信号)',
  PRIMARY KEY (`id`),
  KEY `idx_spu_id` (`spu_id`),
  KEY `idx_category3_id` (`category3_id`),
  KEY `idx_brand_id` (`brand_id`),
  KEY `idx_is_sale` (`is_sale`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SKU表(源/维)';

-- ============ 7. 用户表 ============
CREATE TABLE `user_info` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '用户ID(主键)',
  `login_name`   VARCHAR(50)  NOT NULL                COMMENT '登录账号',
  `nick_name`    VARCHAR(50)  DEFAULT NULL            COMMENT '用户昵称',
  `name`         VARCHAR(50)  DEFAULT NULL            COMMENT '真实姓名',
  `phone_num`    VARCHAR(20)  DEFAULT NULL            COMMENT '手机号',
  `email`        VARCHAR(50)  DEFAULT NULL            COMMENT '邮箱',
  `user_level`   TINYINT      DEFAULT NULL            COMMENT '用户等级,见base_dic:1普通/2青铜/3白银/4黄金/5铂金/6钻石',
  `birthday`     DATE         DEFAULT NULL            COMMENT '出生日期',
  `gender`       TINYINT      DEFAULT NULL            COMMENT '性别:0未知/1男/2女',
  `age_range`    VARCHAR(20)  DEFAULT NULL            COMMENT '年龄段:18-25,26-35...',
  `status`       SMALLINT     DEFAULT NULL            COMMENT '用户状态,见base_dic:1001正常/1002冻结/1003注销',
  `create_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
  `update_time`  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间(维度变更信号)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_login_name` (`login_name`),
  KEY `idx_phone_num` (`phone_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户表(源/维)';


