-- 用户行为日志、业务操作日志(启动日志、页面浏览日志、动作日志、曝光日志、错误日志)
CREATE DATABASE IF NOT EXISTS `gmall_log` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE `gmall_log`;

-- ============ 1. 启动日志 ============
CREATE TABLE `log_startup` (
  `id`        BIGINT      NOT NULL AUTO_INCREMENT COMMENT '主键',
  `mid`       VARCHAR(20) DEFAULT NULL            COMMENT '设备ID',
  `user_id`   BIGINT      DEFAULT NULL            COMMENT '用户ID(对齐业务/基础库BIGINT,未登录为空)',
  `appid`     VARCHAR(20) DEFAULT NULL            COMMENT '应用ID',
  `os`        VARCHAR(20) DEFAULT NULL            COMMENT '操作系统',
  `area`      VARCHAR(20) DEFAULT NULL            COMMENT '地区',
  `version`   VARCHAR(20) DEFAULT NULL            COMMENT '版本号',
  `channel`   VARCHAR(20) DEFAULT NULL            COMMENT '渠道',
  `entry`     VARCHAR(20) DEFAULT NULL            COMMENT '入口类型',
  `loading_time` BIGINT   DEFAULT NULL            COMMENT '启动耗时(ms)',
  `open_ad_id`   VARCHAR(20) DEFAULT NULL        COMMENT '开屏广告ID',
  `open_ad_ms`   BIGINT   DEFAULT NULL            COMMENT '开屏广告耗时(ms)',
  `create_time` DATETIME  NOT NULL                COMMENT '事件时间',
  PRIMARY KEY (`id`),
  KEY `idx_mid` (`mid`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='启动日志(源)';

-- ============ 2. 页面浏览日志 ============
CREATE TABLE `log_page_view` (
  `id`             BIGINT      NOT NULL AUTO_INCREMENT COMMENT '主键',
  `mid`            VARCHAR(20) DEFAULT NULL            COMMENT '设备ID',
  `user_id`        BIGINT      DEFAULT NULL            COMMENT '用户ID(对齐业务/基础库BIGINT,未登录为空)',
  `page_id`        VARCHAR(20) DEFAULT NULL            COMMENT '页面ID',
  `page_name`      VARCHAR(100) DEFAULT NULL           COMMENT '页面名称',
  `last_page_id`   VARCHAR(20) DEFAULT NULL            COMMENT '上一个页面ID',
  `jump_count`     INT         DEFAULT NULL            COMMENT '跳转次数',
  `during_time`    BIGINT      DEFAULT NULL            COMMENT '页面停留时长(ms)',
  `source_type`    VARCHAR(20) DEFAULT NULL            COMMENT '来源类型',
  `create_time`    DATETIME    NOT NULL                COMMENT '事件时间',
  PRIMARY KEY (`id`),
  KEY `idx_mid` (`mid`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='页面浏览日志(源)';

-- ============ 3. 动作日志 ============
CREATE TABLE `log_action` (
  `id`         BIGINT      NOT NULL AUTO_INCREMENT COMMENT '主键',
  `mid`        VARCHAR(20) DEFAULT NULL            COMMENT '设备ID',
  `user_id`    BIGINT      DEFAULT NULL            COMMENT '用户ID(对齐业务/基础库BIGINT,未登录为空)',
  `item_type`  VARCHAR(20) DEFAULT NULL            COMMENT '动作对象类型',
  `item_id`    VARCHAR(20) DEFAULT NULL            COMMENT '动作对象ID(如sku_id)',
  `target_page_id` VARCHAR(20) DEFAULT NULL        COMMENT '目标页面ID',
  `create_time` DATETIME   NOT NULL                COMMENT '事件时间',
  PRIMARY KEY (`id`),
  KEY `idx_mid` (`mid`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='动作日志(源)';

-- ============ 4. 曝光日志 ============
CREATE TABLE `log_display` (
  `id`           BIGINT      NOT NULL AUTO_INCREMENT COMMENT '主键',
  `mid`          VARCHAR(20) DEFAULT NULL            COMMENT '设备ID',
  `user_id`      BIGINT      DEFAULT NULL            COMMENT '用户ID(对齐业务/基础库BIGINT,未登录为空)',
  `page_id`      VARCHAR(20) DEFAULT NULL            COMMENT '页面ID',
  `display_type` VARCHAR(20) DEFAULT NULL            COMMENT '曝光类型',
  `item_ids`     VARCHAR(500) DEFAULT NULL           COMMENT '曝光的商品ID集合(逗号分隔)',
  `item_pos`     VARCHAR(500) DEFAULT NULL           COMMENT '商品所处位置集合',
  `create_time`  DATETIME    NOT NULL                COMMENT '事件时间',
  PRIMARY KEY (`id`),
  KEY `idx_mid` (`mid`),
  KEY `idx_user_id` (`user_id`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='曝光日志(源)';

-- ============ 5. 错误日志 ============
CREATE TABLE `log_error` (
  `id`          BIGINT     NOT NULL AUTO_INCREMENT COMMENT '主键',
  `mid`         VARCHAR(20) DEFAULT NULL            COMMENT '设备ID',
  `user_id`     BIGINT      DEFAULT NULL            COMMENT '用户ID(对齐业务/基础库BIGINT,未登录为空)',
  `appid`       VARCHAR(20) DEFAULT NULL            COMMENT '应用ID',
  `err_code`    VARCHAR(20) DEFAULT NULL            COMMENT '错误码',
  `err_name`    VARCHAR(100) DEFAULT NULL           COMMENT '错误名称',
  `err_content` VARCHAR(500) DEFAULT NULL           COMMENT '错误内容',
  `create_time` DATETIME   NOT NULL                COMMENT '事件时间',
  PRIMARY KEY (`id`),
  KEY `idx_mid` (`mid`),
  KEY `idx_create_time` (`create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='错误日志(源)';



